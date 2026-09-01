package snowflake

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/snowflakedb/gosnowflake"
)

func TestSnowflakeStreamingRequestJournalCommercialRoundTrip(t *testing.T) {
	if strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_MANAGED")) != "1" {
		t.Skip("commercial Snowflake request-journal evidence requires WALLABY_TEST_SNOWFLAKE_MANAGED=1")
	}
	dsn := strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_DSN"))
	parsed, err := gosnowflake.ParseDSN(dsn)
	if err != nil || parsed.Database == "" {
		t.Fatalf("parse commercial Snowflake DSN database: %v", err)
	}
	schema := parsed.Schema
	if schema == "" {
		schema = "PUBLIC"
	}
	policy, err := connector.NewSnowflakeDeploymentPolicy(connector.SnowflakeDeploymentConfig{
		Enabled:        true,
		Account:        strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_ACCOUNT")),
		User:           strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_USER")),
		Host:           strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_HOST")),
		PrivateKeyFile: strings.TrimSpace(os.Getenv("WALLABY_TEST_SNOWFLAKE_PRIVATE_KEY_FILE")),
	})
	if err != nil {
		t.Fatalf("load commercial Snowflake policy: %v", err)
	}
	t.Cleanup(func() {
		if err := policy.Close(); err != nil {
			t.Errorf("close commercial Snowflake policy: %v", err)
		}
	})
	db, err := connector.OpenSnowflakeDB(dsn, policy)
	if err != nil {
		t.Fatalf("open commercial Snowflake request journal: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close commercial Snowflake request journal: %v", err)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping commercial Snowflake request journal: %v", err)
	}

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	cfg := streamConfig{
		database: parsed.Database, schema: schema, pipe: "WALLABY_STREAM_PIPE_" + suffix,
		channelStateTable: "WALLABY_STREAM_CHANNEL_" + suffix,
	}
	channelTable := managedSnowflakeStreamQualifiedTable(cfg, cfg.channelStateTable)
	requestTable := streamRequestTable(cfg)
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), time.Minute)
		defer cleanupCancel()
		for _, statement := range []string{"DROP TABLE IF EXISTS " + requestTable, "DROP TABLE IF EXISTS " + channelTable} {
			if _, err := db.ExecContext(cleanupCtx, statement); err != nil {
				t.Errorf("commercial Snowflake request-journal cleanup %q: %v", statement, err)
			}
		}
	})
	for _, statement := range managedStreamCurrentSchemaDDL(cfg) {
		if _, err := db.ExecContext(ctx, statement); err != nil {
			t.Fatalf("provision commercial Snowflake request journal: %v", err)
		}
	}

	protocol := newSQLStreamProtocol(db)
	request := managedStreamRequest{
		requestID: "wallaby-stream-request-" + strings.Repeat("a", 64),
		flowID:    "commercial-flow", flowIncarnationID: "commercial-incarnation", sourceLineageID: "commercial-lineage",
		destinationRevisionID: "commercial-revision", logicalBatchID: "commercial-batch", positionID: "0/100",
		contentHash: strings.Repeat("b", 64), manifestHash: strings.Repeat("c", 64), rowsContentHash: strings.Repeat("d", 64), rowCount: 1,
		channelName: "COMMERCIAL_CHANNEL", pipeName: cfg.pipe, channelRevision: 1, pipeRevision: "commercial-pipe-revision",
		inputContinuation: "continuation-1", expectedPreviousOffset: "offset-0", requestedOffset: "offset-1",
		generation: 1, acquisitionID: "commercial-acquisition", leaseEpoch: 1, attempt: 1,
		phase: streamRequestPrepared, phaseVersion: 1,
	}
	inserted, err := protocol.InsertRequest(ctx, cfg, request)
	if err != nil || !inserted {
		t.Fatalf("insert commercial Snowflake request journal=%t/%v", inserted, err)
	}
	if inserted, err := protocol.InsertRequest(ctx, cfg, request); err != nil || inserted {
		t.Fatalf("duplicate commercial Snowflake request journal=%t/%v, want false/nil", inserted, err)
	}
	key := streamRequestKey{flowIncarnationID: request.flowIncarnationID, destinationRevisionID: request.destinationRevisionID, logicalBatchID: request.logicalBatchID}
	stored, found, err := protocol.LookupRequest(ctx, cfg, key)
	if err != nil || !found || !sameManagedStreamRequestIdentity(stored, request) {
		t.Fatalf("lookup commercial Snowflake request journal=%+v/%t/%v", stored, found, err)
	}
	transition := streamRequestTransition{requestID: request.requestID, expectedPhase: streamRequestPrepared, expectedVersion: 1, nextPhase: streamRequestSendingUnknown, responseKind: "commercial-send-owner"}
	stored, applied, err := protocol.TransitionRequest(ctx, cfg, transition)
	if err != nil || !applied || stored.phase != streamRequestSendingUnknown || stored.phaseVersion != 2 {
		t.Fatalf("commercial Snowflake request CAS=%+v/%t/%v", stored, applied, err)
	}
	if _, applied, err := protocol.TransitionRequest(ctx, cfg, transition); err != nil || applied {
		t.Fatalf("stale commercial Snowflake request CAS=%t/%v, want false/nil", applied, err)
	}
	channelKey := streamChannelStateKey{flowIncarnationID: request.flowIncarnationID, destinationRevisionID: request.destinationRevisionID, channelName: request.channelName}
	if unresolved, err := protocol.HasUnresolvedRequests(ctx, cfg, channelKey); err != nil || !unresolved {
		t.Fatalf("commercial Snowflake unresolved request=%t/%v, want true/nil", unresolved, err)
	}
	stored, applied, err = protocol.TransitionRequest(ctx, cfg, streamRequestTransition{
		requestID: request.requestID, expectedPhase: streamRequestSendingUnknown, expectedVersion: 2,
		nextPhase: streamRequestCommitted, responseContinuation: "continuation-2", committedOffset: request.requestedOffset,
		responseKind: "commercial-committed", responseEvidence: "journal-round-trip",
	})
	if err != nil || !applied || stored.phase != streamRequestCommitted || stored.phaseVersion != 3 {
		t.Fatalf("commercial Snowflake committed request CAS=%+v/%t/%v", stored, applied, err)
	}
	if unresolved, err := protocol.HasUnresolvedRequests(ctx, cfg, channelKey); err != nil || unresolved {
		t.Fatalf("commercial Snowflake resolved request=%t/%v, want false/nil", unresolved, err)
	}

	state := managedStreamChannelState{
		flowIncarnationID: request.flowIncarnationID, destinationRevisionID: request.destinationRevisionID,
		channelName: request.channelName, pipeName: request.pipeName, pipeRevision: request.pipeRevision,
		channelRevision: request.channelRevision, continuationToken: "continuation-2", committedOffsetToken: request.requestedOffset,
		logicalBatchID: request.logicalBatchID, rowsContentHash: request.rowsContentHash, requestID: request.requestID, stateVersion: 1,
	}
	current, applied, err := protocol.CompareAndSwapChannelState(ctx, cfg, managedStreamChannelState{}, state)
	if err != nil || !applied || current.stateVersion != 1 || current.requestID != request.requestID {
		t.Fatalf("commercial Snowflake channel CAS=%+v/%t/%v", current, applied, err)
	}
	if current, applied, err := protocol.CompareAndSwapChannelState(ctx, cfg, managedStreamChannelState{}, state); err != nil || applied || current.stateVersion != 1 {
		t.Fatalf("stale commercial Snowflake channel CAS=%+v/%t/%v, want current/false/nil", current, applied, err)
	}
}
