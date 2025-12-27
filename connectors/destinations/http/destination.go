package http

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/josephjohncox/ductstream/pkg/connector"
	"github.com/josephjohncox/ductstream/pkg/wire"
)

const (
	optURL               = "url"
	optMethod            = "method"
	optFormat            = "format"
	optTimeout           = "timeout"
	optHeaders           = "headers"
	optMaxRetries        = "max_retries"
	optBackoffBase       = "backoff_base"
	optBackoffMax        = "backoff_max"
	optBackoffFactor     = "backoff_factor"
	optIdempotencyHeader = "idempotency_header"
)

// Destination delivers records to an HTTP endpoint.
type Destination struct {
	spec              connector.Spec
	url               string
	method            string
	codec             wire.Codec
	headers           map[string]string
	client            *http.Client
	maxRetries        int
	backoffBase       time.Duration
	backoffMax        time.Duration
	backoffFactor     float64
	idempotencyHeader string
}

func (d *Destination) Open(_ context.Context, spec connector.Spec) error {
	d.spec = spec
	d.url = spec.Options[optURL]
	if d.url == "" {
		return errors.New("http url is required")
	}

	d.method = strings.ToUpper(spec.Options[optMethod])
	if d.method == "" {
		d.method = http.MethodPost
	}

	format := spec.Options[optFormat]
	if format == "" {
		format = string(connector.WireFormatJSON)
	}
	codec, err := wire.NewCodec(format)
	if err != nil {
		return err
	}
	d.codec = codec

	if spec.Options[optTimeout] == "" {
		d.client = &http.Client{Timeout: 10 * time.Second}
	} else {
		timeout, err := time.ParseDuration(spec.Options[optTimeout])
		if err != nil {
			return fmt.Errorf("parse timeout: %w", err)
		}
		d.client = &http.Client{Timeout: timeout}
	}

	d.headers = parseHeaders(spec.Options[optHeaders])
	d.maxRetries = parseInt(spec.Options[optMaxRetries], 3)
	d.backoffBase = parseDuration(spec.Options[optBackoffBase], 200*time.Millisecond)
	d.backoffMax = parseDuration(spec.Options[optBackoffMax], 5*time.Second)
	d.backoffFactor = parseFloat(spec.Options[optBackoffFactor], 2.0)
	d.idempotencyHeader = spec.Options[optIdempotencyHeader]
	if d.idempotencyHeader == "" {
		d.idempotencyHeader = "Idempotency-Key"
	}

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.client == nil {
		return errors.New("http destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}

	for _, record := range batch.Records {
		payloadBatch := connector.Batch{
			Records:    []connector.Record{record},
			Schema:     batch.Schema,
			Checkpoint: batch.Checkpoint,
			WireFormat: batch.WireFormat,
		}

		payload, err := d.codec.Encode(payloadBatch)
		if err != nil {
			return err
		}
		if len(payload) == 0 {
			continue
		}

		idempotencyKey := d.buildIdempotencyKey(record, batch.Checkpoint.LSN, payload)
		if err := d.sendWithRetry(ctx, payload, idempotencyKey); err != nil {
			return err
		}
	}

	return nil
}

func (d *Destination) ApplyDDL(_ context.Context, _ connector.Schema, _ connector.Record) error {
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) Close(_ context.Context) error {
	return nil
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      true,
		SupportsTypeMapping:   true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatParquet,
			connector.WireFormatAvro,
			connector.WireFormatProto,
			connector.WireFormatJSON,
		},
	}
}

func (d *Destination) sendWithRetry(ctx context.Context, payload []byte, idempotencyKey string) error {
	attempts := d.maxRetries + 1
	if attempts < 1 {
		attempts = 1
	}

	for attempt := 1; attempt <= attempts; attempt++ {
		req, err := http.NewRequestWithContext(ctx, d.method, d.url, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		for k, v := range d.headers {
			req.Header.Set(k, v)
		}
		if d.codec != nil {
			req.Header.Set("Content-Type", d.codec.ContentType())
		}
		if d.idempotencyHeader != "" && idempotencyKey != "" {
			req.Header.Set(d.idempotencyHeader, idempotencyKey)
		}

		resp, err := d.client.Do(req)
		if err == nil && resp != nil {
			_ = resp.Body.Close()
		}

		if err == nil && resp != nil && resp.StatusCode < 300 {
			return nil
		}

		if attempt >= attempts || !retryable(err, resp) {
			if err != nil {
				return err
			}
			if resp != nil {
				return fmt.Errorf("http destination status %d", resp.StatusCode)
			}
			return errors.New("http destination failed")
		}

		sleep := d.backoffDuration(attempt)
		timer := time.NewTimer(sleep)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}

	return errors.New("http destination retries exhausted")
}

func (d *Destination) backoffDuration(attempt int) time.Duration {
	base := float64(d.backoffBase)
	if base <= 0 {
		base = float64(200 * time.Millisecond)
	}
	factor := d.backoffFactor
	if factor <= 0 {
		factor = 2.0
	}
	pow := math.Pow(factor, float64(attempt-1))
	delay := time.Duration(base * pow)
	if d.backoffMax > 0 && delay > d.backoffMax {
		delay = d.backoffMax
	}

	jitter := 0.5 + rand.Float64()
	return time.Duration(float64(delay) * jitter)
}

func (d *Destination) buildIdempotencyKey(record connector.Record, lsn string, payload []byte) string {
	if d.idempotencyHeader == "" {
		return ""
	}

	keyPart := string(record.Key)
	if keyPart == "" {
		keyPart = string(payload)
	}
	base := fmt.Sprintf("%s|%s|%s", record.Table, keyPart, lsn)
	sum := sha256.Sum256([]byte(base))
	return hex.EncodeToString(sum[:])
}

func retryable(err error, resp *http.Response) bool {
	if err != nil {
		return true
	}
	if resp == nil {
		return true
	}

	switch resp.StatusCode {
	case http.StatusTooManyRequests, http.StatusRequestTimeout:
		return true
	default:
		return resp.StatusCode >= 500
	}
}

func parseHeaders(value string) map[string]string {
	out := map[string]string{}
	if value == "" {
		return out
	}
	pairs := strings.Split(value, ",")
	for _, pair := range pairs {
		item := strings.TrimSpace(pair)
		if item == "" {
			continue
		}
		parts := strings.SplitN(item, ":", 2)
		if len(parts) != 2 {
			continue
		}
		key := strings.TrimSpace(parts[0])
		val := strings.TrimSpace(parts[1])
		if key != "" {
			out[key] = val
		}
	}
	return out
}

func parseInt(value string, fallback int) int {
	if value == "" {
		return fallback
	}
	var parsed int
	if _, err := fmt.Sscanf(value, "%d", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func parseFloat(value string, fallback float64) float64 {
	if value == "" {
		return fallback
	}
	var parsed float64
	if _, err := fmt.Sscanf(value, "%f", &parsed); err != nil {
		return fallback
	}
	return parsed
}

func parseDuration(value string, fallback time.Duration) time.Duration {
	if value == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fallback
	}
	return parsed
}
