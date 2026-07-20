package stream

import (
	"context"
	"errors"
	"fmt"
	nethttp "net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"

	httpdest "github.com/josephjohncox/wallaby/connectors/destinations/http"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestWriteWithRetrySerialRetriesFailedAndUnattemptedDestinationsOnly(t *testing.T) {
	t.Parallel()

	destinations, implementations := retryDestinations(4, 1)
	runner := Runner{
		Destinations: destinations,
		Parallelism:  1,
	}
	if err := runner.writeWithRetry(context.Background(), retryTestBatch(), destinations); err != nil {
		t.Fatalf("writeWithRetry() error = %v", err)
	}

	wantCalls := []int{1, 2, 1, 1}
	for index, destination := range implementations {
		if got := destination.callCount(); got != wantCalls[index] {
			t.Fatalf("destination %d calls = %d, want %d", index, got, wantCalls[index])
		}
	}
}

func TestWriteWithRetryParallelRetriesFailedDestinationsOnly(t *testing.T) {
	t.Parallel()

	destinations, implementations := retryDestinations(8, 3)
	runner := Runner{
		Destinations: destinations,
		Parallelism:  len(destinations),
	}
	if err := runner.writeWithRetry(context.Background(), retryTestBatch(), destinations); err != nil {
		t.Fatalf("writeWithRetry() error = %v", err)
	}

	for index, destination := range implementations {
		want := 1
		if index == 3 {
			want = 2
		}
		if got := destination.callCount(); got != want {
			t.Fatalf("destination %d calls = %d, want %d", index, got, want)
		}
	}
}

func TestRunnerAcknowledgesOnlyAfterAllUnresolvedDestinationsSucceed(t *testing.T) {
	t.Parallel()

	batch := retryTestBatch()
	batch.Checkpoint.Metadata = map[string]string{"seq": "1"}
	log := &eventLog{}
	source := &fakeSource{batches: []connector.Batch{batch}, log: log}
	destinations, implementations := retryDestinations(3, 1)
	runner := Runner{
		Source:       source,
		SourceSpec:   connector.Spec{Options: map[string]string{"mode": "backfill"}},
		Destinations: destinations,
		Checkpoints:  &recordingCheckpointStore{},
		FlowID:       "retry-ack-order",
		Parallelism:  1,
	}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if len(source.acks) != 1 {
		t.Fatalf("source acknowledgements = %d, want 1", len(source.acks))
	}
	if got := implementations[1].callCount(); got != 2 {
		t.Fatalf("failed destination calls = %d, want 2 before acknowledgement", got)
	}
	for index, destination := range implementations {
		if index == 1 {
			continue
		}
		if got := destination.callCount(); got != 1 {
			t.Fatalf("successful destination %d calls = %d, want 1", index, got)
		}
	}
}

func TestWriteWithRetryRetriesRealHTTPDestinationAfterConnectorFailure(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		if requests.Add(1) == 1 {
			nethttp.Error(w, "unavailable", nethttp.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := &httpdest.Destination{}
	if err := destination.Open(context.Background(), connector.Spec{Options: map[string]string{
		"url":           server.URL,
		"payload_mode":  "record_json",
		"max_retries":   "0",
		"dedupe_window": "1h",
	}}); err != nil {
		t.Fatalf("open HTTP destination: %v", err)
	}
	defer destination.Close(context.Background())

	config := DestinationConfig{Spec: connector.Spec{Name: "http"}, Dest: destination}
	runner := Runner{Destinations: []DestinationConfig{config}, Parallelism: 1}
	if err := runner.writeWithRetry(context.Background(), retryTestBatch(), runner.Destinations); err != nil {
		t.Fatalf("writeWithRetry() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("HTTP requests = %d, want failed connector attempt plus runner retry", got)
	}
}

func TestRunnerRejectsInvalidBatchBeforeDestinationWriteOrAcknowledgement(t *testing.T) {
	t.Parallel()

	batch := retryTestBatch()
	batch.Records = append(batch.Records, connector.Record{
		Table:         "other_events",
		Operation:     connector.OpInsert,
		SchemaVersion: 1,
	})
	source := &fakeSource{batches: []connector.Batch{batch}, log: &eventLog{}}
	destination := &retryDestination{}
	runner := Runner{
		Source:     source,
		SourceSpec: connector.Spec{Options: map[string]string{"mode": "backfill"}},
		Destinations: []DestinationConfig{{
			Spec: connector.Spec{Name: "destination"},
			Dest: destination,
		}},
		Checkpoints: &recordingCheckpointStore{},
		FlowID:      "invalid-batch",
	}

	err := runner.Run(context.Background())
	if !errors.Is(err, connector.ErrInvalidBatch) {
		t.Fatalf("Run() error = %v, want invalid batch", err)
	}
	if got := destination.callCount(); got != 0 {
		t.Fatalf("destination calls = %d, want 0", got)
	}
	if len(source.acks) != 0 {
		t.Fatalf("source acknowledgements = %d, want 0", len(source.acks))
	}
}

func TestWriteWithRetryCancellationStopsPendingRetry(t *testing.T) {
	t.Parallel()

	firstAttempt := make(chan struct{})
	destination := &retryDestination{failures: 10, firstAttempt: firstAttempt}
	config := DestinationConfig{Spec: connector.Spec{Name: "failing"}, Dest: destination}
	runner := Runner{
		Destinations: []DestinationConfig{config},
		Parallelism:  1,
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- runner.writeWithRetry(ctx, retryTestBatch(), runner.Destinations)
	}()
	<-firstAttempt
	cancel()

	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("writeWithRetry() error = %v, want context canceled", err)
	}
	if got := destination.callCount(); got != 1 {
		t.Fatalf("destination calls after cancellation = %d, want 1", got)
	}
}

func BenchmarkWriteWithRetryOneFailureAmongFour(b *testing.B) {
	benchmarkWriteWithRetryOneFailure(b, 4)
}

func BenchmarkWriteWithRetryOneFailureAmongEight(b *testing.B) {
	benchmarkWriteWithRetryOneFailure(b, 8)
}

func benchmarkWriteWithRetryOneFailure(b *testing.B, count int) {
	b.Helper()
	batch := retryTestBatch()
	destinations := make([]DestinationConfig, 0, count)
	implementations := make([]*retryBenchmarkDestination, 0, count)
	for index := 0; index < count; index++ {
		destination := &retryBenchmarkDestination{failFirst: index == count/2}
		implementations = append(implementations, destination)
		destinations = append(destinations, DestinationConfig{
			Spec: connector.Spec{Name: fmt.Sprintf("destination-%d", index)},
			Dest: destination,
		})
	}
	runner := Runner{Destinations: destinations, Parallelism: count}

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if err := runner.writeWithRetry(context.Background(), batch, destinations); err != nil {
			b.Fatalf("writeWithRetry() error = %v", err)
		}
	}
	b.StopTimer()

	var calls int64
	for _, destination := range implementations {
		calls += destination.calls.Load()
	}
	wantCalls := int64(b.N * (count + 1))
	if calls != wantCalls {
		b.Fatalf("destination calls = %d, want %d", calls, wantCalls)
	}
	b.ReportMetric(float64(calls)/float64(b.N), "destination-calls/op")
}

type retryDestination struct {
	mu           sync.Mutex
	calls        int
	failures     int
	firstAttempt chan struct{}
}

func (d *retryDestination) Open(context.Context, connector.Spec) error { return nil }

func (d *retryDestination) Write(context.Context, connector.Batch) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.calls++
	if d.calls == 1 && d.firstAttempt != nil {
		close(d.firstAttempt)
	}
	if d.failures > 0 {
		d.failures--
		return errors.New("transient destination failure")
	}
	return nil
}

func (d *retryDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}

func (d *retryDestination) TypeMappings() map[string]string { return nil }
func (d *retryDestination) Close(context.Context) error     { return nil }
func (d *retryDestination) Capabilities() connector.Capabilities {
	return retrySafeCapabilities()
}

func (d *retryDestination) callCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.calls
}

type retryBenchmarkDestination struct {
	calls     atomic.Int64
	failFirst bool
}

func (d *retryBenchmarkDestination) Open(context.Context, connector.Spec) error { return nil }
func (d *retryBenchmarkDestination) Write(context.Context, connector.Batch) error {
	call := d.calls.Add(1)
	if d.failFirst && call%2 == 1 {
		return errors.New("transient destination failure")
	}
	return nil
}
func (d *retryBenchmarkDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (d *retryBenchmarkDestination) TypeMappings() map[string]string { return nil }
func (d *retryBenchmarkDestination) Close(context.Context) error     { return nil }
func (d *retryBenchmarkDestination) Capabilities() connector.Capabilities {
	return retrySafeCapabilities()
}

func retryDestinations(count, failingIndex int) ([]DestinationConfig, []*retryDestination) {
	destinations := make([]DestinationConfig, 0, count)
	implementations := make([]*retryDestination, 0, count)
	for index := 0; index < count; index++ {
		destination := &retryDestination{}
		if index == failingIndex {
			destination.failures = 1
		}
		implementations = append(implementations, destination)
		destinations = append(destinations, DestinationConfig{
			Spec: connector.Spec{Name: fmt.Sprintf("destination-%d", index)},
			Dest: destination,
		})
	}
	return destinations, implementations
}

func retrySafeCapabilities() connector.Capabilities {
	return connector.Capabilities{Delivery: connector.DeliverySemantics{
		Declared:         true,
		IdempotentReplay: true,
		ReplaySafe:       true,
	}}
}

func retryTestBatch() connector.Batch {
	return connector.Batch{
		Schema: connector.Schema{Name: "events", Namespace: "public", Version: 1},
		Records: []connector.Record{{
			Table:         "events",
			Operation:     connector.OpInsert,
			SchemaVersion: 1,
			After:         map[string]any{"id": int64(1)},
		}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}
}

// writeDestinations is a one-attempt test seam. Production delivery uses
// writeWithRetry, which carries only unresolved destinations between attempts.
func (r *Runner) writeDestinations(ctx context.Context, batch connector.Batch, destinations []DestinationConfig) error {
	return r.attemptDestinations(ctx, batch, destinations).err
}
