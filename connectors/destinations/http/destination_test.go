package http

import (
	"context"
	"errors"
	nethttp "net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestFailedDeliveryIsNotRemembered(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	var succeed atomic.Bool
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		requests.Add(1)
		if !succeed.Load() {
			nethttp.Error(w, "unavailable", nethttp.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	if err := destination.Write(context.Background(), batch); err == nil {
		t.Fatal("first Write() unexpectedly succeeded")
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests after failed write = %d, want 1", got)
	}

	succeed.Store(true)
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("retry Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after retry = %d, want 2", got)
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("deduplicated Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after completed duplicate = %d, want 2", got)
	}
}

func TestDistinctUpdatesToSameKeyAreDelivered(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	keys := make(chan string, 2)
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, request *nethttp.Request) {
		requests.Add(1)
		keys <- request.Header.Get("Idempotency-Key")
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	batch.Records = []connector.Record{
		{
			Table:          "widgets",
			Operation:      connector.OpUpdate,
			SchemaVersion:  1,
			Key:            []byte("widget-1"),
			After:          map[string]any{"id": int64(1), "status": "first"},
			SourcePosition: "0/10",
		},
		{
			Table:          "widgets",
			Operation:      connector.OpUpdate,
			SchemaVersion:  1,
			Key:            []byte("widget-1"),
			After:          map[string]any{"id": int64(1), "status": "second"},
			SourcePosition: "0/20",
		},
	}
	batch.Checkpoint.LSN = "0/20"

	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests = %d, want both updates delivered", got)
	}
	firstKey, secondKey := <-keys, <-keys
	if firstKey == "" || secondKey == "" || firstKey == secondKey {
		t.Fatalf("idempotency keys = (%q, %q), want distinct non-empty event identities", firstKey, secondKey)
	}

	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatalf("replay Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after exact replay = %d, want 2", got)
	}
}

func TestCompletedDeliveryIsSkippedOnlyUntilExpiry(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		requests.Add(1)
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	now := time.Unix(1_000, 0)
	destination.now = func() time.Time { return now }
	batch := testBatch()

	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests before expiry = %d, want 1", got)
	}

	now = now.Add(2 * time.Hour)
	if err := destination.Write(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after expiry = %d, want 2", got)
	}
}

func TestConcurrentDuplicateRetriesAfterLeaderFailure(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		if requests.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
			nethttp.Error(w, "unavailable", nethttp.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	firstResult := make(chan error, 1)
	secondResult := make(chan error, 1)
	go func() { firstResult <- destination.Write(context.Background(), batch) }()
	<-firstEntered
	go func() { secondResult <- destination.Write(context.Background(), batch) }()

	select {
	case err := <-secondResult:
		t.Fatalf("duplicate returned before leader completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("concurrent requests before leader completion = %d, want 1", got)
	}

	close(releaseFirst)
	if err := <-firstResult; err == nil {
		t.Fatal("leader Write() unexpectedly succeeded")
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("waiting duplicate Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after leader failure = %d, want 2", got)
	}
}

func TestConcurrentDuplicateSkipsAfterLeaderSuccess(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		if requests.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	firstResult := make(chan error, 1)
	secondResult := make(chan error, 1)
	go func() { firstResult <- destination.Write(context.Background(), batch) }()
	<-firstEntered
	go func() { secondResult <- destination.Write(context.Background(), batch) }()
	close(releaseFirst)

	if err := <-firstResult; err != nil {
		t.Fatalf("leader Write() error = %v", err)
	}
	if err := <-secondResult; err != nil {
		t.Fatalf("duplicate Write() error = %v", err)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("requests after leader success = %d, want 1", got)
	}
}

func TestCancellationReleasesReservation(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	server := httptest.NewServer(nethttp.HandlerFunc(func(w nethttp.ResponseWriter, _ *nethttp.Request) {
		if requests.Add(1) == 1 {
			close(firstEntered)
			<-releaseFirst
			return
		}
		w.WriteHeader(nethttp.StatusNoContent)
	}))
	defer server.Close()

	destination := openTestDestination(t, server.URL, time.Hour)
	batch := testBatch()
	leaderContext, cancelLeader := context.WithCancel(context.Background())
	leaderResult := make(chan error, 1)
	waitingResult := make(chan error, 1)
	go func() { leaderResult <- destination.Write(leaderContext, batch) }()
	<-firstEntered
	go func() { waitingResult <- destination.Write(context.Background(), batch) }()
	cancelLeader()
	close(releaseFirst)

	if err := <-leaderResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("leader Write() error = %v, want context canceled", err)
	}
	if err := <-waitingResult; err != nil {
		t.Fatalf("waiting Write() error = %v", err)
	}
	if got := requests.Load(); got != 2 {
		t.Fatalf("requests after cancellation = %d, want 2", got)
	}
}

func openTestDestination(t *testing.T, url string, dedupeWindow time.Duration) *Destination {
	t.Helper()
	destination := &Destination{}
	err := destination.Open(context.Background(), connector.Spec{Options: map[string]string{
		optURL:          url,
		optPayloadMode:  payloadModeRecordJSON,
		optMaxRetries:   "0",
		optDedupeWindow: dedupeWindow.String(),
	}})
	if err != nil {
		t.Fatalf("Open() error = %v", err)
	}
	return destination
}

func testBatch() connector.Batch {
	return connector.Batch{
		Schema: connector.Schema{Name: "widgets", Namespace: "public", Version: 1},
		Records: []connector.Record{{
			Table:         "widgets",
			Operation:     connector.OpInsert,
			SchemaVersion: 1,
			After:         map[string]any{"id": int64(1)},
		}},
		Checkpoint: connector.Checkpoint{LSN: "0/10"},
	}
}
