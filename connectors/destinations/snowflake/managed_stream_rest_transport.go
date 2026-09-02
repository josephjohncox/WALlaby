package snowflake

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

const (
	streamRESTMaxAppendBytes   = 4 << 20
	streamRESTMaxResponseBytes = 1 << 20
)

// streamRESTTokenProvider returns a fresh key-pair JWT. The transport exchanges
// it for an ingest-host-scoped token and never persists either credential.
type streamRESTTokenProvider interface {
	KeypairJWT(context.Context) (string, error)
}

// streamRESTTransport implements Snowflake's v2 high-performance REST protocol.
// It is deliberately not connected to sqlStreamProtocol until same-SHA
// commercial evidence promotes streamingTransportLinked.
type streamRESTTransport struct {
	client      *http.Client
	controlBase *url.URL
	tokens      streamRESTTokenProvider
	maxResponse int64
	mu          sync.Mutex
	ingestBase  *url.URL
	scopedToken string
}

// Compile the concrete adapter as an interface implementation without linking
// it into runtime construction or changing the promotion gate.
var _ streamTransport = (*streamRESTTransport)(nil)
var _ = newStreamRESTTransport
var _ = (*streamRESTTransport).DropChannel

func newStreamRESTTransport(controlBase string, client *http.Client, tokens streamRESTTokenProvider) (*streamRESTTransport, error) {
	if client == nil || tokens == nil {
		return nil, errors.New("snowpipe Streaming REST transport requires an HTTP client and token provider")
	}
	base, err := url.Parse(strings.TrimSpace(controlBase))
	if err != nil || base.Host == "" || base.Opaque != "" || base.Path != "" && base.Path != "/" || base.RawQuery != "" || base.Fragment != "" {
		return nil, errors.New("snowpipe Streaming control endpoint is malformed")
	}
	if err := validateStreamRESTEndpoint(base); err != nil {
		return nil, err
	}
	copyClient := *client
	if !isStreamRESTLoopback(base.Hostname()) {
		transport := client.Transport
		if transport == nil {
			transport = http.DefaultTransport
		}
		httpTransport, ok := transport.(*http.Transport)
		if !ok {
			return nil, errors.New("snowpipe Streaming production client requires a reviewable HTTP transport")
		}
		if httpTransport.TLSClientConfig != nil && httpTransport.TLSClientConfig.InsecureSkipVerify {
			return nil, errors.New("snowpipe Streaming production client rejects TLS verification bypass")
		}
		clonedTransport := httpTransport.Clone()
		clonedTransport.Proxy = nil
		if clonedTransport.TLSClientConfig == nil {
			clonedTransport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		} else {
			clonedTransport.TLSClientConfig = clonedTransport.TLSClientConfig.Clone()
			if clonedTransport.TLSClientConfig.MinVersion < tls.VersionTLS12 {
				clonedTransport.TLSClientConfig.MinVersion = tls.VersionTLS12
			}
		}
		copyClient.Transport = clonedTransport
	}
	copyClient.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }
	return &streamRESTTransport{client: &copyClient, controlBase: base, tokens: tokens, maxResponse: streamRESTMaxResponseBytes}, nil
}

func validateStreamRESTEndpoint(endpoint *url.URL) error {
	if endpoint == nil || endpoint.Hostname() == "" || endpoint.User != nil {
		return errors.New("snowpipe Streaming endpoint is missing")
	}
	if endpoint.Scheme == "https" {
		if !isStreamRESTLoopback(endpoint.Hostname()) {
			if endpoint.Port() != "" && endpoint.Port() != "443" {
				return errors.New("snowpipe Streaming production endpoint requires HTTPS port 443")
			}
			if !isAllowedStreamRESTSnowflakeHost(endpoint.Hostname()) {
				return errors.New("snowpipe Streaming endpoint is outside the Snowflake origin allowlist")
			}
		}
		return nil
	}
	if endpoint.Scheme == "http" && isStreamRESTLoopback(endpoint.Hostname()) {
		return nil
	}
	return errors.New("snowpipe Streaming REST requires HTTPS; HTTP is loopback-test-only")
}

func isAllowedStreamRESTSnowflakeHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	return strings.HasSuffix(host, ".snowflakecomputing.com") && host != "snowflakecomputing.com"
}

func streamRESTAccountLabel(host string) string {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	label, _, _ := strings.Cut(host, ".")
	return strings.ReplaceAll(label, "_", "-")
}

func isStreamRESTLoopback(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (t *streamRESTTransport) validateConfigAccount(cfg streamConfig) error {
	if isStreamRESTLoopback(t.controlBase.Hostname()) {
		return nil
	}
	configured, err := connector.SnowflakeRESTAccountLabel(cfg.account)
	if err != nil || streamRESTAccountLabel(t.controlBase.Hostname()) != configured {
		return errors.New("snowpipe Streaming control origin does not match the complete admitted Snowflake account identifier")
	}
	return nil
}

func (t *streamRESTTransport) OpenChannel(ctx context.Context, cfg streamConfig, channelName string) (streamChannelStatus, error) {
	if err := t.validateConfigAccount(cfg); err != nil {
		return streamChannelStatus{}, err
	}
	if strings.TrimSpace(channelName) == "" {
		return streamChannelStatus{}, errors.New("snowpipe Streaming channel name is required")
	}
	ingest, token, err := t.session(ctx, false)
	if err != nil {
		return streamChannelStatus{}, err
	}
	requestID := streamRESTUUID("open\x1f" + cfg.destinationRevision + "\x1f" + channelName)
	path := streamRESTChannelPath(cfg, channelName)
	query := url.Values{"requestId": {requestID}}
	body := []byte(`{"fail_on_uncommitted_rows":true}`)
	var response streamRESTOpenResponse
	status, err := t.doJSON(ctx, http.MethodPut, ingest, path, query, token, "", "application/json", body, &response)
	if err != nil {
		return streamChannelStatus{}, err
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		t.invalidateSession()
		return streamChannelStatus{}, errStreamAuthExpired
	}
	if status == http.StatusConflict {
		return streamChannelStatus{}, errStreamChannelInvalidated
	}
	if status < 200 || status >= 300 {
		return streamChannelStatus{}, classifyStreamRESTStatus(status, "open channel")
	}
	if err := response.validate(cfg, channelName); err != nil {
		return streamChannelStatus{}, err
	}
	return streamRESTStatus(cfg, response.NextContinuationToken, response.ChannelStatus), nil
}

func (t *streamRESTTransport) AppendRows(ctx context.Context, req streamAppendRequest) (streamAppendResult, error) {
	if err := t.validateConfigAccount(req.cfg); err != nil {
		return streamAppendResult{}, newStreamAppendFailure(streamAppendFailurePreSend, err)
	}
	if req.requestID == "" || req.continuationToken == "" || req.offsetToken == "" || req.expectedPreviousOffset == req.offsetToken || req.rowCount != len(req.rows) || req.rowCount == 0 {
		return streamAppendResult{}, newStreamAppendFailure(streamAppendFailurePreSend, errors.New("snowpipe Streaming append request is incomplete"))
	}
	payload, err := streamRESTNDJSON(req.rows)
	if err != nil {
		return streamAppendResult{}, newStreamAppendFailure(streamAppendFailurePreSend, err)
	}
	ingest, token, err := t.session(ctx, false)
	if err != nil {
		return streamAppendResult{}, newStreamAppendFailure(streamAppendFailurePreSend, err)
	}
	query := url.Values{
		"continuationToken": {req.continuationToken},
		"startOffsetToken":  {req.offsetToken},
		"endOffsetToken":    {req.offsetToken},
		"requestId":         {streamRESTUUID(req.requestID)},
	}
	path := streamRESTAppendPath(req.cfg, req.channelName)
	var response streamRESTAppendResponse
	status, requestErr := t.doJSON(ctx, http.MethodPost, ingest, path, query, token, "", "application/x-ndjson", payload, &response)
	if requestErr != nil {
		return streamAppendResult{disposition: streamAppendUnknown, requestID: req.requestID}, newStreamAppendFailure(streamAppendFailureAmbiguous, fmt.Errorf("%w: append rows: %w", connector.ErrDeliveryIndeterminate, requestErr))
	}
	switch {
	case status >= 200 && status < 300:
		if strings.TrimSpace(response.NextContinuationToken) == "" || response.NextContinuationToken == req.continuationToken {
			return streamAppendResult{}, newStreamAppendFailure(streamAppendFailureAmbiguous, fmt.Errorf("%w: append response continuation token did not advance", connector.ErrDeliveryConflict))
		}
		return streamAppendResult{disposition: streamAppendAccepted, requestID: req.requestID, continuationToken: response.NextContinuationToken, evidence: "snowflake-rest-accepted"}, nil
	case status == http.StatusUnauthorized || status == http.StatusForbidden:
		t.invalidateSession()
		return streamAppendResult{}, newStreamAppendFailure(streamAppendFailureDefinitelyNotAccepted, errStreamAuthExpired)
	case status == http.StatusConflict:
		return streamAppendResult{}, newStreamAppendFailure(streamAppendFailureDefinitelyNotAccepted, errStreamChannelInvalidated)
	case status == http.StatusRequestTimeout || status == http.StatusTooManyRequests || status >= 500:
		if status == http.StatusTooManyRequests {
			return streamAppendResult{}, newStreamAppendFailure(streamAppendFailureDefinitelyNotAccepted, errStreamThrottled)
		}
		return streamAppendResult{disposition: streamAppendUnknown, requestID: req.requestID}, newStreamAppendFailure(streamAppendFailureAmbiguous, fmt.Errorf("%w: Snowpipe Streaming append returned HTTP %d", connector.ErrDeliveryIndeterminate, status))
	case status >= 400 && status < 500:
		// The public API does not provide a per-request non-acceptance lookup.
		// A client error can arrive after the service consumed request bytes, so
		// keep the request unresolved unless a future authoritative API proves it absent.
		return streamAppendResult{disposition: streamAppendUnknown, requestID: req.requestID}, newStreamAppendFailure(streamAppendFailureAmbiguous, fmt.Errorf("%w: Snowpipe Streaming append returned HTTP %d", connector.ErrDeliveryIndeterminate, status))
	default:
		return streamAppendResult{}, newStreamAppendFailure(streamAppendFailureAmbiguous, fmt.Errorf("%w: Snowpipe Streaming append returned HTTP %d", connector.ErrDeliveryIndeterminate, status))
	}
}

func (t *streamRESTTransport) ChannelStatus(ctx context.Context, cfg streamConfig, channelName string) (streamChannelStatus, error) {
	if err := t.validateConfigAccount(cfg); err != nil {
		return streamChannelStatus{}, err
	}
	ingest, token, err := t.session(ctx, false)
	if err != nil {
		return streamChannelStatus{}, err
	}
	body, err := json.Marshal(struct {
		ChannelNames []string `json:"channel_names"`
	}{ChannelNames: []string{channelName}})
	if err != nil {
		return streamChannelStatus{}, err
	}
	var response streamRESTBulkStatusResponse
	status, err := t.doJSON(ctx, http.MethodPost, ingest, streamRESTBulkStatusPath(cfg), nil, token, "", "application/json", body, &response)
	if err != nil {
		return streamChannelStatus{}, err
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		t.invalidateSession()
		return streamChannelStatus{}, errStreamAuthExpired
	}
	if status < 200 || status >= 300 {
		return streamChannelStatus{}, classifyStreamRESTStatus(status, "channel status")
	}
	if len(response.ChannelStatuses) > 1 {
		return streamChannelStatus{}, fmt.Errorf("%w: Snowpipe Streaming status returned unrequested channels", connector.ErrDeliveryConflict)
	}
	value, ok := response.ChannelStatuses[channelName]
	if !ok {
		return streamChannelStatus{valid: false, channelName: channelName, pipeRevision: cfg.pipeCreatedOn}, nil
	}
	if err := value.validate(cfg, channelName); err != nil {
		return streamChannelStatus{}, err
	}
	return streamRESTStatus(cfg, "", value), nil
}

func (t *streamRESTTransport) RequestStatus(ctx context.Context, cfg streamConfig, request managedStreamRequest) (streamRequestStatusEvidence, error) {
	status, err := t.ChannelStatus(ctx, cfg, request.channelName)
	if err != nil {
		return streamRequestStatusEvidence{}, err
	}
	evidence := streamRequestStatusEvidence{
		disposition: streamRequestUnknown, requestID: request.requestID, channelName: request.channelName,
		pipeName: request.pipeName, channelRevision: request.channelRevision, pipeRevision: request.pipeRevision,
		inputContinuation: request.inputContinuation, expectedPreviousOffset: request.expectedPreviousOffset, requestedOffset: request.requestedOffset,
		manifestHash: request.manifestHash, rowsContentHash: request.rowsContentHash, rowCount: request.rowCount,
		detail: "Snowflake channel status has not committed the exact offset",
	}
	if !status.valid || status.pipeRevision != request.pipeRevision || status.channelRevision != request.channelRevision {
		evidence.disposition = streamRequestStatusDivergent
		return evidence, nil
	}
	if status.committedOffsetToken == request.requestedOffset {
		responseContinuation := request.responseContinuation
		if responseContinuation == "" {
			reopened, openErr := t.OpenChannel(ctx, cfg, request.channelName)
			if openErr != nil {
				return streamRequestStatusEvidence{}, fmt.Errorf("%w: reopen committed Snowpipe Streaming channel: %w", connector.ErrDeliveryIndeterminate, openErr)
			}
			if !reopened.valid || reopened.channelRevision != request.channelRevision || reopened.committedOffsetToken != request.requestedOffset || reopened.pipeRevision != request.pipeRevision || reopened.continuationToken == "" {
				return streamRequestStatusEvidence{}, fmt.Errorf("%w: reopened Snowpipe Streaming channel evidence diverged", connector.ErrDeliveryConflict)
			}
			responseContinuation = reopened.continuationToken
		}
		evidence.disposition = streamRequestStatusCommitted
		evidence.committedOffset = status.committedOffsetToken
		evidence.responseContinuation = responseContinuation
		evidence.detail = "Snowflake channel committed the exact requested offset"
	} else {
		evidence.committedOffset = status.committedOffsetToken
		if status.committedOffsetToken == request.expectedPreviousOffset {
			evidence.detail = "Snowflake channel remains at the exact prior committed offset"
		} else if status.committedOffsetToken != "" {
			evidence.detail = "Snowflake channel reports an opaque offset that cannot be ordered against this request"
		}
	}
	return evidence, nil
}

func (t *streamRESTTransport) DropChannel(ctx context.Context, cfg streamConfig, channelName string) error {
	if err := t.validateConfigAccount(cfg); err != nil {
		return err
	}
	ingest, token, err := t.session(ctx, false)
	if err != nil {
		return err
	}
	query := url.Values{"requestId": {streamRESTUUID("drop\x1f" + cfg.destinationRevision + "\x1f" + channelName)}}
	body := []byte(`{"fail_on_uncommitted_rows":true}`)
	status, err := t.doJSON(ctx, http.MethodDelete, ingest, streamRESTChannelPath(cfg, channelName), query, token, "", "application/json", body, nil)
	if err != nil {
		return err
	}
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		t.invalidateSession()
		return errStreamAuthExpired
	}
	if status == http.StatusConflict {
		return errStreamChannelInvalidated
	}
	if status < 200 || status >= 300 {
		return classifyStreamRESTStatus(status, "drop channel")
	}
	return nil
}

func (t *streamRESTTransport) session(ctx context.Context, force bool) (*url.URL, string, error) { //nolint:unparam // forced refresh is exercised by auth-recovery tests and the experimental runtime.
	t.mu.Lock()
	defer t.mu.Unlock()
	if !force && t.ingestBase != nil && t.scopedToken != "" {
		return cloneURL(t.ingestBase), t.scopedToken, nil
	}
	jwt, err := t.tokens.KeypairJWT(ctx)
	if err != nil || strings.TrimSpace(jwt) == "" {
		return nil, "", errors.New("snowpipe Streaming key-pair JWT is unavailable")
	}
	var hostResponse streamRESTHostnameResponse
	status, err := t.doJSON(ctx, http.MethodGet, t.controlBase, "/v2/streaming/hostname", nil, jwt, "KEYPAIR_JWT", "", nil, &hostResponse)
	if err != nil {
		return nil, "", err
	}
	if status < 200 || status >= 300 || strings.TrimSpace(hostResponse.Hostname) == "" {
		return nil, "", classifyStreamRESTStatus(status, "discover ingest hostname")
	}
	ingest, err := t.validatedIngestURL(hostResponse.Hostname)
	if err != nil {
		return nil, "", err
	}
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:jwt-bearer"}, "scope": {ingest.Host}}
	var tokenResponse streamRESTTokenResponse
	status, err = t.doJSON(ctx, http.MethodPost, t.controlBase, "/oauth/token", nil, jwt, "KEYPAIR_JWT", "application/x-www-form-urlencoded", []byte(form.Encode()), &tokenResponse)
	if err != nil {
		return nil, "", err
	}
	if status < 200 || status >= 300 || strings.TrimSpace(tokenResponse.Token) == "" {
		return nil, "", classifyStreamRESTStatus(status, "exchange scoped token")
	}
	t.ingestBase, t.scopedToken = ingest, tokenResponse.Token
	return cloneURL(ingest), tokenResponse.Token, nil
}

func (t *streamRESTTransport) invalidateSession() {
	t.mu.Lock()
	t.scopedToken = ""
	t.mu.Unlock()
}

func (t *streamRESTTransport) validatedIngestURL(raw string) (*url.URL, error) {
	raw = strings.TrimSpace(strings.Trim(raw, `"`))
	if raw == "" || strings.Contains(raw, "://") || strings.ContainsAny(raw, "/?#@") {
		return nil, errors.New("snowpipe Streaming ingest hostname is malformed")
	}
	// Snowflake documents underscore-to-hyphen normalization for the returned
	// ingest hostname. Apply it only to that discovered hostname.
	host := strings.ReplaceAll(raw, "_", "-")
	parsed, err := url.Parse("//" + host)
	if err != nil || parsed.User != nil || parsed.Hostname() == "" || parsed.Path != "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("snowpipe Streaming ingest hostname is malformed")
	}
	ingest := &url.URL{Scheme: t.controlBase.Scheme, Host: parsed.Host}
	if err := validateStreamRESTEndpoint(ingest); err != nil {
		return nil, err
	}
	controlHost := strings.ToLower(t.controlBase.Hostname())
	ingestHost := strings.ToLower(ingest.Hostname())
	if isStreamRESTLoopback(controlHost) {
		if ingestHost != controlHost || ingest.Port() != t.controlBase.Port() {
			return nil, errors.New("snowpipe Streaming loopback ingest host drifted from the control origin")
		}
		return ingest, nil
	}
	if streamRESTAccountLabel(ingestHost) != streamRESTAccountLabel(controlHost) {
		return nil, errors.New("snowpipe Streaming ingest host belongs to a different Snowflake account")
	}
	return ingest, nil
}

func (t *streamRESTTransport) doJSON(ctx context.Context, method string, base *url.URL, path string, query url.Values, token, tokenType, contentType string, body []byte, output any) (int, error) {
	endpoint := cloneURL(base)
	endpoint.Path = path
	endpoint.RawQuery = query.Encode()
	request, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(body))
	if err != nil {
		return 0, err
	}
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
		if tokenType != "" {
			request.Header.Set("X-Snowflake-Authorization-Token-Type", tokenType)
		}
	}
	if contentType != "" {
		request.Header.Set("Content-Type", contentType)
	}
	response, err := t.client.Do(request)
	if err != nil {
		return 0, err
	}
	defer func() { _ = response.Body.Close() }()
	limited := io.LimitReader(response.Body, t.maxResponse+1)
	payload, err := io.ReadAll(limited)
	if err != nil {
		return response.StatusCode, err
	}
	if int64(len(payload)) > t.maxResponse {
		return response.StatusCode, errors.New("snowpipe Streaming response exceeds the configured bound")
	}
	if output != nil && len(bytes.TrimSpace(payload)) > 0 && response.StatusCode >= 200 && response.StatusCode < 300 {
		decoder := json.NewDecoder(bytes.NewReader(payload))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(output); err != nil {
			return response.StatusCode, fmt.Errorf("decode Snowpipe Streaming response: %w", err)
		}
		if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
			return response.StatusCode, errors.New("snowpipe Streaming response contains trailing JSON")
		}
	}
	return response.StatusCode, nil
}

func streamRESTNDJSON(rows []streamAppendRow) ([]byte, error) {
	var buffer bytes.Buffer
	for _, row := range rows {
		if len(row.payload) == 0 || !json.Valid(row.payload) {
			return nil, errors.New("snowpipe Streaming row payload is not valid JSON")
		}
		if buffer.Len()+len(row.payload)+1 > streamRESTMaxAppendBytes {
			return nil, errStreamOversize
		}
		buffer.Write(row.payload)
		buffer.WriteByte('\n')
	}
	return buffer.Bytes(), nil
}

func streamRESTUUID(identity string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte(identity)).String()
}

func streamRESTRevision(createdOnMS int64) int64 {
	if createdOnMS <= 0 {
		return 0
	}
	return createdOnMS
}

func streamRESTStatus(cfg streamConfig, continuation string, value streamRESTChannelStatus) streamChannelStatus {
	return streamChannelStatus{
		valid: strings.EqualFold(value.ChannelStatusCode, "ACTIVE"), channelName: value.ChannelName,
		channelRevision: streamRESTRevision(value.CreatedOnMS), pipeRevision: cfg.pipeCreatedOn,
		continuationToken: continuation, committedOffsetToken: value.LastCommittedOffsetToken,
	}
}

func streamRESTChannelPath(cfg streamConfig, channel string) string {
	return "/v2/streaming/databases/" + url.PathEscape(cfg.database) + "/schemas/" + url.PathEscape(cfg.schema) + "/pipes/" + url.PathEscape(cfg.pipe) + "/channels/" + url.PathEscape(channel)
}

func streamRESTAppendPath(cfg streamConfig, channel string) string {
	return "/v2/streaming/data/databases/" + url.PathEscape(cfg.database) + "/schemas/" + url.PathEscape(cfg.schema) + "/pipes/" + url.PathEscape(cfg.pipe) + "/channels/" + url.PathEscape(channel) + "/rows"
}

func streamRESTBulkStatusPath(cfg streamConfig) string {
	return "/v2/streaming/databases/" + url.PathEscape(cfg.database) + "/schemas/" + url.PathEscape(cfg.schema) + "/pipes/" + url.PathEscape(cfg.pipe) + ":bulk-channel-status"
}

func cloneURL(value *url.URL) *url.URL {
	copyValue := *value
	return &copyValue
}

func classifyStreamRESTStatus(status int, operation string) error {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return errStreamAuthExpired
	case http.StatusConflict:
		return errStreamChannelInvalidated
	case http.StatusRequestTimeout, http.StatusTooManyRequests:
		return errStreamThrottled
	default:
		return fmt.Errorf("%w: %s returned HTTP %d", connector.ErrDeliveryIndeterminate, operation, status)
	}
}

type streamRESTHostnameResponse struct {
	Hostname string `json:"hostname"`
}
type streamRESTTokenResponse struct {
	Token string `json:"token"`
}
type streamRESTAppendResponse struct {
	NextContinuationToken string `json:"next_continuation_token"`
}
type streamRESTOpenResponse struct {
	NextContinuationToken string                  `json:"next_continuation_token"`
	ChannelStatus         streamRESTChannelStatus `json:"channel_status"`
}
type streamRESTBulkStatusResponse struct {
	ChannelStatuses map[string]streamRESTChannelStatus `json:"channel_statuses"`
}
type streamRESTChannelStatus struct {
	DatabaseName              string `json:"database_name"`
	SchemaName                string `json:"schema_name"`
	PipeName                  string `json:"pipe_name"`
	ChannelName               string `json:"channel_name"`
	ChannelStatusCode         string `json:"channel_status_code"`
	LastCommittedOffsetToken  string `json:"last_committed_offset_token"`
	CreatedOnMS               int64  `json:"created_on_ms"`
	RowsInserted              int64  `json:"rows_inserted"`
	RowsParsed                int64  `json:"rows_parsed"`
	RowsErrors                int64  `json:"rows_errors"`
	RowsErrorCount            int64  `json:"rows_error_count"`
	LastErrorOffsetUpperBound string `json:"last_error_offset_upper_bound"`
	LastErrorMessage          string `json:"last_error_message"`
	LastErrorTimestamp        string `json:"last_error_timestamp"`
	ProcessingLatencyMS       int64  `json:"snowflake_avg_processing_latency_ms"`
}

func (r streamRESTOpenResponse) validate(cfg streamConfig, channel string) error {
	if strings.TrimSpace(r.NextContinuationToken) == "" || r.ChannelStatus.CreatedOnMS <= 0 {
		return errors.New("snowpipe Streaming open response omitted continuation token")
	}
	return r.ChannelStatus.validate(cfg, channel)
}

func (s streamRESTChannelStatus) validate(cfg streamConfig, channel string) error {
	if s.ChannelName != channel || !strings.EqualFold(s.DatabaseName, cfg.database) || !strings.EqualFold(s.SchemaName, cfg.schema) || !strings.EqualFold(s.PipeName, cfg.pipe) || s.ChannelStatusCode == "" || s.CreatedOnMS <= 0 || s.RowsInserted < 0 || s.RowsParsed < 0 || s.RowsErrors < 0 || s.RowsErrorCount < 0 {
		return fmt.Errorf("%w: Snowpipe Streaming channel status identity is incomplete", connector.ErrDeliveryConflict)
	}
	if s.RowsErrors > 0 || s.RowsErrorCount > 0 || s.LastErrorOffsetUpperBound != "" || s.LastErrorMessage != "" {
		return fmt.Errorf("%w: Snowpipe Streaming channel status reports row errors", connector.ErrDeliveryConflict)
	}
	return nil
}
