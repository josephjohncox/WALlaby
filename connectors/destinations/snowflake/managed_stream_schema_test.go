package snowflake

import (
	"context"
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
		"STATE_VERSION", "REQUEST_ID", "INPUT_CONTINUATION_TOKEN", "REQUESTED_OFFSET_TOKEN",
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
	_, _, err = newSQLStreamProtocol(db).LookupRequest(context.Background(), cfg, streamRequestKey{flowIncarnationID: request.flowIncarnationID, destinationRevisionID: request.destinationRevisionID, logicalBatchID: request.logicalBatchID})
	if !errors.Is(err, connector.ErrDeliveryConflict) {
		t.Fatalf("duplicate SQL request rows error=%v, want conflict", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestStreamRequestSQLUsesCASPredicates(t *testing.T) {
	cfg := streamTestConfig(t)
	for name, contract := range map[string]struct {
		query    string
		required []string
	}{
		"channel": {query: streamChannelStateMergeSQL(cfg), required: []string{"STATE_VERSION", "EXPECTED_VERSION", "EXPECTED_CHANNEL_REVISION", "EXPECTED_CONTINUATION_TOKEN", "EXPECTED_COMMITTED_OFFSET_TOKEN", "EXPECTED_LOGICAL_BATCH_ID", "EXPECTED_ROWS_CONTENT_HASH"}},
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
	for _, required := range []string{`"STATE_VERSION" = ?`, `"CHANNEL_REVISION" = ?`, `"CONTINUATION_TOKEN" = ?`, `"COMMITTED_OFFSET_TOKEN" = ?`, `NOT EXISTS`, `SENDING_UNKNOWN`, `COMMITTED`} {
		if !strings.Contains(cleanup, required) {
			t.Fatalf("channel cleanup CAS SQL missing %q: %s", required, cleanup)
		}
	}
}
