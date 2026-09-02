package snowflake

import (
	"context"
	"database/sql"
	sqldriver "database/sql/driver"
	"errors"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedStreamCurrentSchemaContainsRequestAuthority(t *testing.T) {
	cfg := streamTestConfig(t)
	ddl := strings.Join(managedStreamCurrentSchemaDDL(cfg), "\n")
	for _, required := range []string{
		"STATE_VERSION", "REQUEST_ID", "PIPE_NAME", "INPUT_CONTINUATION_TOKEN", "EXPECTED_PREVIOUS_COMMITTED_OFFSET_TOKEN", "REQUESTED_OFFSET_TOKEN",
		"MANIFEST_HASH", "ROWS_CONTENT_HASH", "GENERATION", "ACQUISITION_ID", "LEASE_EPOCH",
		"CREATE HYBRID TABLE", "WALLABY_STREAM_REQUEST_PK", "WALLABY_STREAM_REQUEST_ATTEMPT",
	} {
		if !strings.Contains(ddl, required) {
			t.Fatalf("current streaming schema missing %q", required)
		}
	}
	for _, forbidden := range []string{"IF NOT EXISTS", "NOT ENFORCED", "legacy", "fallback"} {
		if strings.Contains(ddl, forbidden) {
			t.Fatalf("current streaming schema contains compatibility token %q", forbidden)
		}
	}
}

func TestSQLStreamRequestLookupRejectsDuplicateAuthorityRows(t *testing.T) {
	cfg, _, _, plan := streamTestFixture(t)
	request, err := newManagedStreamRequest(plan, streamChannelStatus{valid: true, channelName: plan.identity.channelName, channelRevision: 1, pipeRevision: "pipe-rev-1", continuationToken: "cont-1"}, 1)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	values := streamRequestValues(request)
	driverValues := make([]sqldriver.Value, len(values))
	for index := range values {
		driverValues[index] = values[index]
	}
	rows := sqlmock.NewRows(streamRequestColumns()).AddRow(driverValues...).AddRow(driverValues...)
	mock.ExpectQuery(regexp.QuoteMeta(streamRequestLookupSQL(cfg))).WithArgs(request.flowIncarnationID, request.destinationRevisionID, request.logicalBatchID).WillReturnRows(rows)
	_, _, err = newSQLStreamProtocol(func(ctx context.Context) (*sql.Conn, error) { return db.Conn(ctx) }).LookupRequest(context.Background(), cfg, streamRequestKey{flowIncarnationID: request.flowIncarnationID, destinationRevisionID: request.destinationRevisionID, logicalBatchID: request.logicalBatchID})
	if !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("duplicate SQL request rows error=%v, want conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStreamProtocolAcquiresValidatedConnectionForEveryAuthorityOperation(t *testing.T) {
	cfg := streamTestConfig(t)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	key := streamChannelStateKey{flowIncarnationID: "incarnation", destinationRevisionID: "revision", channelName: "channel"}
	for range 2 {
		mock.ExpectQuery(regexp.QuoteMeta(streamRequestUnresolvedSQL(cfg))).
			WithArgs(key.flowIncarnationID, key.destinationRevisionID, key.channelName).
			WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0))
	}
	acquisitions := 0
	protocol := newSQLStreamProtocol(func(ctx context.Context) (*sql.Conn, error) {
		acquisitions++
		// Production supplies acquireValidatedStreamConn here. Returning a fresh
		// sql.Conn in this contract test proves the protocol never retains or
		// bypasses the acquisition authority between operations.
		return db.Conn(ctx)
	})
	for range 2 {
		unresolved, err := protocol.HasUnresolvedRequests(context.Background(), cfg, key)
		if err != nil || unresolved {
			t.Fatalf("unresolved request lookup=%t/%v", unresolved, err)
		}
	}
	if acquisitions != 2 {
		t.Fatalf("validated connection acquisitions=%d, want one per authority operation", acquisitions)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestSQLStreamChannelCASUsesAffectedRowsNotPostRead(t *testing.T) {
	cfg := streamTestConfig(t)
	state := managedStreamChannelState{
		flowIncarnationID: "11111111-1111-1111-1111-111111111111", destinationRevisionID: "revision", channelName: "channel",
		pipeName: cfg.pipe, pipeRevision: "pipe-rev-1", channelRevision: 1, continuationToken: "cont-1",
		logicalBatchID: "logical", rowsContentHash: strings.Repeat("a", 64), requestID: "request", stateVersion: 1,
	}
	for _, test := range []struct {
		name     string
		affected int64
		wantErr  bool
	}{
		{name: "matching post-read is not ownership", affected: 0},
		{name: "multiple affected rows conflict", affected: 2, wantErr: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			db, mock, err := sqlmock.New()
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			values := append(streamChannelStateValues(state), int64(0), "", "", int64(0), "", "", "", "", "")
			args := make([]sqldriver.Value, len(values))
			for index := range values {
				args[index] = values[index]
			}
			mock.ExpectExec(regexp.QuoteMeta(streamChannelStateMergeSQL(cfg))).WithArgs(args...).WillReturnResult(sqlmock.NewResult(0, test.affected))
			if test.affected <= 1 {
				rowValues := streamChannelStateValues(state)
				row := make([]sqldriver.Value, len(rowValues))
				for index := range rowValues {
					row[index] = rowValues[index]
				}
				mock.ExpectQuery(regexp.QuoteMeta(streamChannelStateLookupSQL(cfg))).WithArgs(state.flowIncarnationID, state.destinationRevisionID, state.channelName).WillReturnRows(sqlmock.NewRows(streamChannelStateColumns()).AddRow(row...))
			}
			_, applied, err := newSQLStreamProtocol(func(ctx context.Context) (*sql.Conn, error) { return db.Conn(ctx) }).CompareAndSwapChannelState(context.Background(), cfg, managedStreamChannelState{}, state)
			if test.wantErr {
				if !errors.Is(err, connector.ErrDeliveryConflict) {
					t.Fatalf("CAS error=%v, want conflict", err)
				}
			} else if err != nil || applied {
				t.Fatalf("CAS applied/error=%t/%v, matching post-read must not prove ownership", applied, err)
			}
			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestStreamRequestSQLUsesCASPredicates(t *testing.T) {
	cfg := streamTestConfig(t)
	for name, contract := range map[string]struct {
		query    string
		required []string
	}{
		"channel": {query: streamChannelStateMergeSQL(cfg), required: []string{"STATE_VERSION", "EXPECTED_VERSION", "EXPECTED_PIPE_NAME", "EXPECTED_PIPE_REVISION", "EXPECTED_CHANNEL_REVISION", "EXPECTED_CONTINUATION_TOKEN", "EXPECTED_COMMITTED_OFFSET_TOKEN", "EXPECTED_LOGICAL_BATCH_ID", "EXPECTED_ROWS_CONTENT_HASH", "EXPECTED_REQUEST_ID"}},
		"request": {query: streamRequestTransitionSQL(cfg), required: []string{"PHASE_VERSION", "PHASE"}},
	} {
		for _, required := range contract.required {
			if !strings.Contains(contract.query, required) {
				t.Fatalf("%s CAS SQL missing %q: %s", name, required, contract.query)
			}
		}
	}
	if !strings.Contains(streamRequestTransitionSQL(cfg), `"PHASE_VERSION" = ?`) || !strings.Contains(streamRequestTransitionSQL(cfg), `"PHASE" = ?`) {
		t.Fatal("request transition is not phase/version compare-and-swap")
	}
	cleanup := streamChannelStateDeleteCASSQL(cfg)
	for _, required := range []string{`"STATE_VERSION" = ?`, `"PIPE_NAME" = ?`, `"PIPE_REVISION" = ?`, `"CHANNEL_REVISION" = ?`, `"CONTINUATION_TOKEN" = ?`, `"COMMITTED_OFFSET_TOKEN" = ?`, `"REQUEST_ID" = ?`, `NOT EXISTS`, `SENDING_UNKNOWN`, `COMMITTED`} {
		if !strings.Contains(cleanup, required) {
			t.Fatalf("channel cleanup CAS SQL missing %q: %s", required, cleanup)
		}
	}
}
