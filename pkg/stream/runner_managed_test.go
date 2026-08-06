package stream

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedPostgresProfileRejectsUnprovenMixedMajorPair(t *testing.T) {
	t.Parallel()
	if err := validateManagedPostgresMajorPair(14, 17); err == nil {
		t.Fatal("mixed PostgreSQL major pair was admitted without executable matrix evidence")
	}
	if err := validateManagedPostgresMajorPair(16, 16); err != nil {
		t.Fatalf("same-major PostgreSQL pair rejected: %v", err)
	}
}

func TestManagedClickHouseProfileRejectsUnprovenVersionPair(t *testing.T) {
	t.Parallel()
	if err := validateManagedClickHouseVersionPair(16, "25.12.1.649"); err != nil {
		t.Fatalf("proven PostgreSQL/ClickHouse pair rejected: %v", err)
	}
	if err := validateManagedClickHouseVersionPair(15, "25.12.1.649"); err == nil {
		t.Fatal("unproven PostgreSQL major was admitted")
	}
	if err := validateManagedClickHouseVersionPair(16, "25.12.10.7"); err == nil {
		t.Fatal("unproven ClickHouse patch was admitted")
	}
}

func TestManagedSnowflakePublicationRequiresExactlyTheAdmittedRelation(t *testing.T) {
	t.Parallel()
	if err := validateManagedSnowflakePublicationRelation(connector.ManagedProfilePostgresToSnowflakeSQLV1, []string{`"public"."widgets"`}, "public", "widgets"); err != nil {
		t.Fatal(err)
	}
	for _, tables := range [][]string{nil, {`"public"."widgets"`, `"public"."audit"`}, {`"other"."widgets"`}} {
		if err := validateManagedSnowflakePublicationRelation(connector.ManagedProfilePostgresToSnowflakeStagedAppendV1, tables, "public", "widgets"); err == nil {
			t.Fatalf("publication tables %v were admitted", tables)
		}
	}
}

func TestManagedSnowflakePublicationPreservesExactWhitespaceIdentifiers(t *testing.T) {
	t.Parallel()
	for _, relation := range []struct{ schema, table string }{{" ", " "}, {" leading", "trailing "}, {" both ", " all "}} {
		expected := pgx.Identifier{relation.schema, relation.table}.Sanitize()
		if err := validateManagedSnowflakePublicationRelation(connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1, []string{expected}, relation.schema, relation.table); err != nil {
			t.Fatalf("exact publication relation %q rejected: %v", expected, err)
		}
		trimmed := pgx.Identifier{strings.TrimSpace(relation.schema), strings.TrimSpace(relation.table)}.Sanitize()
		if trimmed != expected {
			if err := validateManagedSnowflakePublicationRelation(connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1, []string{trimmed}, relation.schema, relation.table); err == nil {
				t.Fatalf("trimmed publication relation %q admitted for %q", trimmed, expected)
			}
		}
	}
}

func TestManagedSnowflakeProfileRequiresPostgres16AndExactRuntimePin(t *testing.T) {
	t.Parallel()
	if err := validateManagedSnowflakeVersionPair(connector.ManagedProfilePostgresToSnowflakeSQLV1, 16, "9.99.0", "9.99.0"); err != nil {
		t.Fatalf("exact runtime pin rejected: %v", err)
	}
	if err := validateManagedSnowflakeVersionPair(connector.ManagedProfilePostgresToSnowflakeStagedAppendV1, 15, "9.99.0", "9.99.0"); err == nil {
		t.Fatal("unproven PostgreSQL major was admitted")
	}
	if err := validateManagedSnowflakeVersionPair(connector.ManagedProfilePostgresToSnowflakeSQLV1, 16, "9.99.1", "9.99.0"); err == nil {
		t.Fatal("Snowflake service version outside the exact runtime pin was admitted")
	}
}

func TestManagedRestoreInitializesDestinationBeforeAckValidationOrFeedback(t *testing.T) {
	t.Parallel()
	events := []string{}
	source := &managedTestSource{events: &events, initial: connector.Checkpoint{LSN: "0/10"}}
	coordinator := &managedTestCoordinator{events: &events, validateErr: errors.New("missing ACK intent")}
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, coordinator, managedTestCheckpointStore{checkpoint: connector.Checkpoint{LSN: "0/10"}})

	err := runner.Run(context.Background())
	if err == nil || !errors.Is(err, coordinator.validateErr) {
		t.Fatalf("Run() error=%v, want missing ACK intent", err)
	}
	if source.acks != 0 {
		t.Fatalf("source ACK calls=%d, want zero before intent validation", source.acks)
	}
	wantPrefix := []string{"destination.open", "destination.initialize", "source.open", "coordinator.validate"}
	if len(events) < len(wantPrefix) || !reflect.DeepEqual(events[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("managed restore events=%v, want destination authority before source and ACK validation: %v", events, wantPrefix)
	}
}

func TestManagedRestoreSeedsSourceWithDeliveredSchemaBaselines(t *testing.T) {
	t.Parallel()
	events := []string{}
	source := &managedTestSource{events: &events, initial: connector.Checkpoint{LSN: "0/10"}}
	coordinator := &managedTestCoordinator{events: &events}
	loaded := []connector.Schema{{Name: "widgets", Namespace: "public", Version: 4, Columns: []connector.Column{{Name: "id", Type: "bigint"}}}}
	baselineBytes, err := json.Marshal(loaded)
	if err != nil {
		t.Fatal(err)
	}
	baseline := string(baselineBytes)
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, coordinator, managedTestCheckpointStore{checkpoint: connector.Checkpoint{LSN: "0/10"}})
	runner.SchemaBaselines = &managedTestSchemaBaselines{load: loaded}
	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := source.openSpec.Options[connector.ManagedSchemaBaselinesOptionKey]; got != baseline {
		t.Fatalf("source schema baseline option=%q, want %q", got, baseline)
	}
}

func TestManagedSnowflakeAppendProfilesMapPolicyWithoutDoubleMappingRawData(t *testing.T) {
	t.Parallel()
	projector := rawAppendRenameProjector{}
	schema := connector.Schema{Namespace: "public", Name: "widgets", Version: 1, Columns: []connector.Column{
		{Name: "id", Type: "int8"}, {Name: "payload", Type: "text"}, {Name: "secret", Type: "text"},
	}}
	transaction := connector.SourceTransaction{
		SourceLineageID: "source/publication-v1", TransactionID: 7,
		BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20", Checkpoint: connector.Checkpoint{LSN: "0/20"},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
			Schema:  schema,
			Records: []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"id": int64(1), "payload": "visible", "secret": "raw-only"}}},
		}}},
	}
	for _, profile := range []string{
		connector.ManagedProfilePostgresToSnowflakeStagedAppendV1,
		connector.ManagedProfilePostgresToSnowflakeStreamingRestAppendV1,
	} {
		t.Run(profile, func(t *testing.T) {
			projected, decision, err := projectManagedRawAppendTransaction(projector, transaction)
			if err != nil {
				t.Fatal(err)
			}
			if decision != ProjectionIncluded || len(projected.Fragments) != 1 {
				t.Fatalf("raw append projection decision/fragments=%v/%d", decision, len(projected.Fragments))
			}
			batch := projected.Fragments[0].Batch
			if batch.Schema.Namespace != "public" || batch.Schema.Name != "widgets" || len(batch.Schema.Columns) != 3 {
				t.Fatalf("raw schema was double-mapped: %+v", batch.Schema)
			}
			after := batch.Records[0].After
			if after["id"] != int64(1) || after["secret"] != "raw-only" || after["EVENT_ID"] != nil || after["PAYLOAD"] != nil {
				t.Fatalf("raw changelog image was renamed/subset before planner encoding: %+v", after)
			}
			if batch.WritePolicy.Mode != connector.ResolvedWriteAppend || batch.WritePolicy.ProjectionFingerprint != projector.Fingerprint() {
				t.Fatalf("append policy was not mapping-bound: %+v", batch.WritePolicy)
			}
		})
	}
}

type rawAppendRenameProjector struct{}

func (rawAppendRenameProjector) Fingerprint() string { return "rename-subset-v1" }
func (rawAppendRenameProjector) IncludeBootstrapRelation(namespace, table string) (bool, error) {
	return namespace == "public" && table == "widgets", nil
}
func (rawAppendRenameProjector) ProjectBootstrapSchema(connector.Schema) (connector.Schema, connector.TableWritePolicy, bool, error) {
	return connector.Schema{Namespace: "PUBLIC", Name: "WALLABY_CHANGELOG", Columns: []connector.Column{{Name: "EVENT_ID", Type: "int8"}, {Name: "PAYLOAD", Type: "text"}}}, connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "rename-subset-v1"}, true, nil
}
func (p rawAppendRenameProjector) ProjectBootstrapBatch(batch connector.Batch) (connector.Batch, bool, error) {
	projected, decision, err := p.ProjectBatch(batch)
	return projected, decision == ProjectionIncluded, err
}
func (rawAppendRenameProjector) ProjectBatch(batch connector.Batch) (connector.Batch, ProjectionDecision, error) {
	batch.Schema.Namespace, batch.Schema.Name = "PUBLIC", "WALLABY_CHANGELOG"
	batch.WritePolicy = connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend, ProjectionFingerprint: "rename-subset-v1"}
	for index := range batch.Records {
		batch.Records[index].Table = "WALLABY_CHANGELOG"
	}
	return batch, ProjectionIncluded, nil
}
func (rawAppendRenameProjector) ProjectTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, ProjectionDecision, error) {
	out := transaction
	out.Fragments[0].Batch.Schema = connector.Schema{Namespace: "PUBLIC", Name: "WALLABY_CHANGELOG", Columns: []connector.Column{{Name: "EVENT_ID", Type: "int8"}}}
	out.Fragments[0].Batch.Records[0].After = map[string]any{"EVENT_ID": int64(1)}
	return out, ProjectionIncluded, nil
}

func TestManagedSnowflakeAppendProfilesProjectStructuredDDLPolicyOnce(t *testing.T) {
	t.Parallel()
	transaction := connector.SourceTransaction{
		SourceLineageID: "source/publication-v1", TransactionID: 8,
		BeginLSN: "0/30", CommitLSN: "0/40", EndLSN: "0/40", Checkpoint: connector.Checkpoint{LSN: "0/40"},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
			Schema:  connector.Schema{Namespace: "public", Name: "widgets", Version: 2, Columns: []connector.Column{{Name: "id", Type: "int8"}}},
			Records: []connector.Record{{Table: "widgets", Operation: connector.OpDDL, DDLPlan: json.RawMessage(`{"Changes":[{"Type":"add_column","Namespace":"public","Table":"widgets","Column":"added","ToType":"text"}]}`)}},
		}}},
	}
	projected, decision, err := projectManagedRawAppendTransaction(rawAppendRenameProjector{}, transaction)
	if err != nil {
		t.Fatal(err)
	}
	if decision != ProjectionIncluded || projected.Fragments[0].Batch.Schema.Name != "WALLABY_CHANGELOG" || projected.Fragments[0].Batch.Records[0].Table != "WALLABY_CHANGELOG" {
		t.Fatalf("structured DDL mapping was not applied exactly once: %+v", projected)
	}
	if projected.Fragments[0].Batch.WritePolicy.ProjectionFingerprint != "rename-subset-v1" {
		t.Fatalf("structured DDL policy is not mapping-bound: %+v", projected.Fragments[0].Batch.WritePolicy)
	}
}

type filteringManagedProjector struct{}

func (filteringManagedProjector) Fingerprint() string { return "filter-v1" }
func (filteringManagedProjector) ProjectBatch(batch connector.Batch) (connector.Batch, ProjectionDecision, error) {
	return connector.Batch{Checkpoint: batch.Checkpoint}, ProjectionFiltered, nil
}
func (filteringManagedProjector) ProjectTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, ProjectionDecision, error) {
	transaction.Fragments = nil
	return transaction, ProjectionFiltered, nil
}

func TestManagedFilteredProjectionAuthorizesAckWithoutDeliveryAttempt(t *testing.T) {
	events := []string{}
	source := &managedTestSource{events: &events, transactions: []connector.SourceTransaction{{
		SourceLineageID: "lineage-1", TransactionID: 9, BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20", Checkpoint: connector.Checkpoint{LSN: "0/20"},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{Schema: connector.Schema{Name: "secret", Namespace: "private", Columns: []connector.Column{{Name: "id", Type: "int8"}}}, Records: []connector.Record{{Table: "secret", Operation: connector.OpInsert, After: map[string]any{"id": int64(1)}}}}}},
	}}}
	coordinator := &managedTestCoordinator{events: &events}
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, coordinator, managedTestCheckpointStore{checkpoint: connector.Checkpoint{LSN: "0/10"}})
	runner.Destinations[0].Projector = filteringManagedProjector{}
	runner.Destinations[0].MappingFingerprint = "filter-v1"
	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !containsEvent(events, "coordinator.authorize") || source.acks != 2 {
		t.Fatalf("events=%v acks=%d, want restore and filtered ACK without delivery", events, source.acks)
	}
}

func TestManagedCancellationDominatesSourceFeedbackTransportError(t *testing.T) {
	t.Parallel()
	events := []string{}
	ctx, cancel := context.WithCancel(context.Background())
	source := &managedTestSource{events: &events, transactions: []connector.SourceTransaction{{
		SourceLineageID: "lineage-1", TransactionID: 7,
		BeginLSN: "0/10", CommitLSN: "0/20", EndLSN: "0/20",
		Checkpoint: connector.Checkpoint{LSN: "0/20"},
	}}}
	feedbackCalls := 0
	coordinator := &managedTestCoordinator{events: &events, commitFeedback: func() error {
		feedbackCalls++
		if feedbackCalls == 1 {
			return nil
		}
		cancel()
		return errors.New("replication stream stopped before source feedback was sent")
	}}
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, coordinator, managedTestCheckpointStore{checkpoint: connector.Checkpoint{LSN: "0/10"}})
	if err := runner.Run(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Run() error=%v, want context cancellation rather than shutdown transport error", err)
	}
}

func TestManagedDestinationInitializationFailsBeforeAllSourceIO(t *testing.T) {
	t.Parallel()
	for _, bootstrapMode := range []string{"never", "required"} {
		t.Run(bootstrapMode, func(t *testing.T) {
			events := []string{}
			initializeErr := errors.New("receipt authority missing")
			source := &managedTestSource{events: &events}
			runner := managedTestRunner(source, &managedTestDestination{events: &events, initializeErr: initializeErr}, &managedTestCoordinator{events: &events}, managedTestCheckpointStore{err: connector.ErrCheckpointNotFound})
			runner.SourceSpec.Options["bootstrap"] = bootstrapMode
			if err := runner.Run(context.Background()); !errors.Is(err, initializeErr) {
				t.Fatalf("Run() error=%v, want initialization failure", err)
			}
			if !reflect.DeepEqual(events, []string{"destination.open", "destination.initialize", "destination.close"}) {
				t.Fatalf("managed initialization failure performed source I/O for bootstrap=%s: %v", bootstrapMode, events)
			}
		})
	}
}

func TestManagedBootstrapPublishesBeforeCDCSourceOpen(t *testing.T) {
	t.Parallel()
	events := []string{}
	source := &managedTestSource{
		events: &events,
		bootstrapResult: connector.ManagedBootstrapResult{
			SourceOptions: map[string]string{"slot": "owned-slot", "publication": "owned-publication", "start_lsn": "0/18", "create_slot": "false"},
			Checkpoint:    connector.Checkpoint{LSN: "0/18"}, CheckpointValid: true,
		},
	}
	coordinator := &managedTestCoordinator{events: &events}
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, coordinator, managedTestCheckpointStore{err: connector.ErrCheckpointNotFound})
	runner.SourceSpec.Options["bootstrap"] = "required"

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"destination.open", "destination.initialize", "source.bootstrap", "source.open", "coordinator.validate", "source.ack", "coordinator.receipt"}
	if len(events) < len(wantPrefix) || !reflect.DeepEqual(events[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("managed bootstrap startup events=%v, want prefix %v", events, wantPrefix)
	}
	if source.openSpec.Options["slot"] != "owned-slot" || source.openSpec.Options["start_lsn"] != "0/18" {
		t.Fatalf("source Open options=%v, want authoritative bootstrap options", source.openSpec.Options)
	}
}

func TestManagedMaterializedRestoresBackpressureBeforeBootstrapWork(t *testing.T) {
	t.Parallel()

	events := []string{}
	source := &managedTestSource{
		events: &events,
		bootstrapResult: connector.ManagedBootstrapResult{
			SourceOptions: map[string]string{"slot": "owned-slot", "start_lsn": "0/18"},
			Checkpoint:    connector.Checkpoint{LSN: "0/18"}, CheckpointValid: true,
		},
	}
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, &managedTestCoordinator{events: &events}, managedTestCheckpointStore{err: connector.ErrCheckpointNotFound})
	runner.SourceSpec.Options["bootstrap"] = "required"
	runner.AckPolicy = AckPolicyMaterialized
	runner.ArtifactLog = &managedTestArtifactLog{events: &events}

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"artifact.recover", "artifact.wait", "destination.open", "source.bootstrap"}
	if len(events) < len(wantPrefix) || !reflect.DeepEqual(events[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("materialized bootstrap startup events=%v, want prefix %v", events, wantPrefix)
	}
}

func TestManagedBootstrapNeverInitializesDestinationBeforeSourceAndInitialCut(t *testing.T) {
	t.Parallel()
	events := []string{}
	source := &managedTestSource{events: &events, initial: connector.Checkpoint{LSN: "0/20"}}
	coordinator := &managedTestCoordinator{events: &events}
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, coordinator, managedTestCheckpointStore{err: connector.ErrCheckpointNotFound})

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"destination.open", "destination.initialize", "source.open", "coordinator.authorize", "coordinator.validate", "source.ack", "coordinator.receipt", "source.read-transaction"}
	if len(events) < len(wantPrefix) || !reflect.DeepEqual(events[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("managed startup events=%v, want prefix %v", events, wantPrefix)
	}
}

func TestManagedMaterializedRestoredPublicationIsValidatedBeforeSourceOpen(t *testing.T) {
	t.Parallel()

	events := []string{}
	restored := connector.Checkpoint{LSN: "0/18", Metadata: map[string]string{
		"artifact_publication_id": "9d5f8653-2bc9-4a83-a967-3da7b4ca68bb",
	}}
	source := &managedTestSource{events: &events}
	coordinator := &managedTestCoordinator{events: &events}
	artifactLog := &managedTestArtifactLog{events: &events}
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, coordinator, managedTestCheckpointStore{checkpoint: restored})
	runner.AckPolicy = AckPolicyMaterialized
	runner.ArtifactLog = artifactLog

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	restoreIndex := eventIndex(events, "artifact.restore")
	openIndex := eventIndex(events, "source.open")
	ackIndex := eventIndex(events, "source.ack")
	if restoreIndex < 0 || openIndex < 0 || ackIndex < 0 || restoreIndex >= openIndex || restoreIndex >= ackIndex {
		t.Fatalf("events=%v, want artifact restore validation before source open and feedback", events)
	}
	if artifactLog.restores != 1 || artifactLog.appends != 0 {
		t.Fatalf("artifact restore/append=%d/%d, want 1/0", artifactLog.restores, artifactLog.appends)
	}
}

func TestManagedMaterializedPublishesBeforeSourceFeedbackWithoutOpeningDestination(t *testing.T) {
	t.Parallel()

	events := []string{}
	source := &managedTestSource{events: &events, transactions: []connector.SourceTransaction{{
		SourceLineageID: "lineage-1", TransactionID: 10,
		BeginLSN: "0/11", CommitLSN: "0/17", EndLSN: "0/18",
		Checkpoint: connector.Checkpoint{LSN: "0/18"},
		Fragments: []connector.TransactionFragment{{Ordinal: 0, Batch: connector.Batch{
			Schema: connector.Schema{Namespace: "public", Name: "events", Columns: []connector.Column{{
				Name: "id", Type: "int8", TypeMetadata: map[string]string{"source_relation_id": "42", "source_column_id": "1"},
			}}},
			Records: []connector.Record{{Table: "events", Operation: connector.OpInsert, After: map[string]any{"id": int64(1)}}},
		}}},
	}}}
	destination := &managedTestDestination{events: &events}
	coordinator := &managedTestCoordinator{events: &events}
	artifactLog := &managedTestArtifactLog{events: &events}
	runner := managedTestRunner(source, destination, coordinator, managedTestCheckpointStore{
		checkpoint: connector.Checkpoint{LSN: "0/10"},
	})
	runner.AckPolicy = AckPolicyMaterialized
	runner.ArtifactLog = artifactLog

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if containsEvent(events, "destination.open") {
		t.Fatalf("materialized worker opened synchronous destination: %v", events)
	}
	appendIndexes := make([]int, 0, 2)
	ackIndexes := make([]int, 0, 2)
	for index, event := range events {
		switch event {
		case "artifact.append":
			appendIndexes = append(appendIndexes, index)
		case "source.ack":
			ackIndexes = append(ackIndexes, index)
		}
	}
	if len(appendIndexes) != 2 || len(ackIndexes) != 2 || appendIndexes[0] >= ackIndexes[0] || appendIndexes[1] >= ackIndexes[1] {
		t.Fatalf("events=%v, want startup and transaction publication before corresponding source ACKs", events)
	}
	if artifactLog.recoveries != 1 || artifactLog.waits < 2 || artifactLog.appends != 2 {
		t.Fatalf("artifact calls recover/wait/append=%d/%d/%d", artifactLog.recoveries, artifactLog.waits, artifactLog.appends)
	}
}

type managedTestArtifactLog struct {
	events     *[]string
	recoveries int
	restores   int
	waits      int
	appends    int
}

func (l *managedTestArtifactLog) Recover(context.Context, connector.RunFence) error {
	l.recoveries++
	*l.events = append(*l.events, "artifact.recover")
	return nil
}

func (l *managedTestArtifactLog) RestoreCheckpoint(_ context.Context, _ connector.RunFence, checkpoint connector.Checkpoint) (connector.AckGrant, error) {
	l.restores++
	*l.events = append(*l.events, "artifact.restore")
	positionID, err := connector.CheckpointPositionID(checkpoint)
	return connector.AckGrant{Checkpoint: checkpoint, PositionID: positionID}, err
}

func (l *managedTestArtifactLog) WaitForReadAdmission(context.Context, connector.RunFence) error {
	l.waits++
	*l.events = append(*l.events, "artifact.wait")
	return nil
}

func (l *managedTestArtifactLog) Append(_ context.Context, _ connector.RunFence, transaction connector.SourceTransaction, _ connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error) {
	l.appends++
	*l.events = append(*l.events, "artifact.append")
	positionID, err := connector.CheckpointPositionID(transaction.Checkpoint)
	return connector.AckGrant{Checkpoint: transaction.Checkpoint, PositionID: positionID}, err
}

type managedBootstrapTestProjector struct{}

func (managedBootstrapTestProjector) Fingerprint() string { return "managed-bootstrap-test-v1" }
func (managedBootstrapTestProjector) IncludeBootstrapRelation(string, string) (bool, error) {
	return true, nil
}
func (managedBootstrapTestProjector) ProjectBatch(batch connector.Batch) (connector.Batch, ProjectionDecision, error) {
	return batch, ProjectionIncluded, nil
}
func (managedBootstrapTestProjector) ProjectTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, ProjectionDecision, error) {
	return transaction, ProjectionIncluded, nil
}
func (managedBootstrapTestProjector) ProjectBootstrapSchema(schema connector.Schema) (connector.Schema, connector.TableWritePolicy, bool, error) {
	return schema, connector.TableWritePolicy{Mode: connector.ResolvedWriteAppend}, true, nil
}
func (managedBootstrapTestProjector) ProjectBootstrapBatch(batch connector.Batch) (connector.Batch, bool, error) {
	return batch, true, nil
}

func managedTestRunner(source connector.Source, destination connector.Destination, coordinator ManagedDeliveryCoordinator, checkpoints connector.CheckpointStore) Runner {
	fence := connector.RunFence{
		FlowID: "managed-test", FlowIncarnationID: uuid.New(), Generation: 1,
		AcquisitionID: uuid.New(), ExecutionID: "execution", LeaseEpoch: 1,
	}
	return Runner{
		Source: source,
		SourceSpec: connector.Spec{Options: map[string]string{
			"managed": "true", "bootstrap": "never", "source_lineage_id": "lineage-1",
		}},
		Destinations:        []DestinationConfig{{Dest: destination, Spec: connector.Spec{Options: map[string]string{"destination_revision_id": "destination-1"}}, Projector: managedBootstrapTestProjector{}, MappingFingerprint: "managed-bootstrap-test-v1"}},
		Checkpoints:         checkpoints,
		FlowID:              fence.FlowID,
		AckPolicy:           AckPolicyAll,
		RunFence:            &fence,
		DeliveryCoordinator: coordinator,
		SchemaBaselines:     &managedTestSchemaBaselines{},
	}
}

type managedTestSchemaBaselines struct {
	load []connector.Schema
}

func (s *managedTestSchemaBaselines) Load(context.Context, connector.RunFence, string) ([]connector.Schema, error) {
	return append([]connector.Schema(nil), s.load...), nil
}

type managedTestSource struct {
	events          *[]string
	initial         connector.Checkpoint
	bootstrapResult connector.ManagedBootstrapResult
	openSpec        connector.Spec
	acks            int
	transactions    []connector.SourceTransaction
	transactionRead int
}

func (s *managedTestSource) Open(_ context.Context, spec connector.Spec) error {
	s.openSpec = spec
	*s.events = append(*s.events, "source.open")
	return nil
}
func (s *managedTestSource) PrepareManagedBootstrap(context.Context, connector.RunFence, connector.Spec, string, connector.ManagedBootstrapProjector, connector.ManagedBootstrapDestination) (connector.ManagedBootstrapResult, error) {
	*s.events = append(*s.events, "source.bootstrap")
	return s.bootstrapResult, nil
}
func (s *managedTestSource) Read(context.Context) (connector.Batch, error) {
	return connector.Batch{}, io.EOF
}
func (s *managedTestSource) ReadTransaction(context.Context) (connector.SourceTransaction, error) {
	*s.events = append(*s.events, "source.read-transaction")
	if s.transactionRead >= len(s.transactions) {
		return connector.SourceTransaction{}, io.EOF
	}
	transaction := s.transactions[s.transactionRead]
	s.transactionRead++
	return transaction, nil
}
func (s *managedTestSource) InitialCheckpoint() (connector.Checkpoint, bool) {
	return s.initial, s.initial.LSN != ""
}
func (s *managedTestSource) Ack(_ context.Context, _ connector.Checkpoint) error {
	s.acks++
	*s.events = append(*s.events, "source.ack")
	return nil
}
func (s *managedTestSource) AckWithEvidence(ctx context.Context, checkpoint connector.Checkpoint) (connector.SourceFlushEvidence, error) {
	if err := s.Ack(ctx, checkpoint); err != nil {
		return connector.SourceFlushEvidence{}, err
	}
	return connector.SourceFlushEvidence{ObservedFlushLSN: checkpoint.LSN}, nil
}
func (s *managedTestSource) Close(context.Context) error {
	*s.events = append(*s.events, "source.close")
	return nil
}
func (*managedTestSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, SupportsStreaming: true}
}

type managedTestDestination struct {
	events        *[]string
	initializeErr error
}

func (d *managedTestDestination) Open(context.Context, connector.Spec) error {
	*d.events = append(*d.events, "destination.open")
	return nil
}
func (*managedTestDestination) Write(context.Context, connector.Batch) error { return nil }
func (*managedTestDestination) ApplyDDL(context.Context, connector.Schema, connector.Record) error {
	return nil
}
func (*managedTestDestination) TypeMappings() map[string]string { return nil }
func (*managedTestDestination) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, Delivery: connector.DeliverySemantics{TransactionalBatch: true, IdempotentReplay: true, ReplaySafe: true}}
}
func (d *managedTestDestination) Close(context.Context) error {
	*d.events = append(*d.events, "destination.close")
	return nil
}
func (*managedTestDestination) Apply(context.Context, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}
func (d *managedTestDestination) InitializeManagedDelivery(context.Context) error {
	*d.events = append(*d.events, "destination.initialize")
	return d.initializeErr
}
func (*managedTestDestination) ValidateTransaction(context.Context, connector.SourceTransaction) error {
	return nil
}
func (*managedTestDestination) ApplyTransaction(context.Context, connector.DeliveryIntent, connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}
func (*managedTestDestination) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
}
func (*managedTestDestination) PrepareBootstrap(context.Context, connector.BootstrapIntent, []connector.BootstrapTable) error {
	return nil
}
func (*managedTestDestination) ApplyBootstrap(context.Context, connector.BootstrapIntent, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}
func (*managedTestDestination) ReconcileBootstrap(context.Context, connector.BootstrapIntent, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
}
func (*managedTestDestination) PublishBootstrap(context.Context, connector.BootstrapIntent, []connector.BootstrapTable) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}
func (*managedTestDestination) ReconcileBootstrapPublication(context.Context, connector.BootstrapIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
}
func (*managedTestDestination) AbandonBootstrap(context.Context, connector.BootstrapIntent, []connector.BootstrapTable) error {
	return nil
}

type managedTestCoordinator struct {
	events         *[]string
	validateErr    error
	commitFeedback func() error
}

func (c *managedTestCoordinator) AuthorizeAck(_ context.Context, _ connector.RunFence, checkpoint connector.Checkpoint, _ connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error) {
	*c.events = append(*c.events, "coordinator.authorize")
	position, err := connector.CheckpointPositionID(checkpoint)
	return connector.AckGrant{Checkpoint: checkpoint, PositionID: position}, err
}
func (*managedTestCoordinator) DeliverTransaction(context.Context, connector.RunFence, connector.DeliveryIntent, connector.SourceTransaction, connector.ManagedSchemaBaselinePayload, connector.ManagedTransactionDestination) (connector.AckGrant, error) {
	return connector.AckGrant{}, errors.New("unexpected transaction delivery")
}
func (c *managedTestCoordinator) ValidateAckGrant(context.Context, connector.RunFence, connector.AckGrant) error {
	*c.events = append(*c.events, "coordinator.validate")
	return c.validateErr
}
func (c *managedTestCoordinator) RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error {
	*c.events = append(*c.events, "coordinator.receipt")
	return nil
}
func (c *managedTestCoordinator) CommitSourceFeedback(ctx context.Context, _ connector.RunFence, grant connector.AckGrant, source connector.FlushEvidenceSource) error {
	*c.events = append(*c.events, "coordinator.validate")
	if c.validateErr != nil {
		return c.validateErr
	}
	if c.commitFeedback != nil {
		return c.commitFeedback()
	}
	if _, err := source.AckWithEvidence(ctx, grant.Checkpoint); err != nil {
		return err
	}
	*c.events = append(*c.events, "coordinator.receipt")
	return nil
}

type managedTestCheckpointStore struct {
	checkpoint connector.Checkpoint
	err        error
}

func (s managedTestCheckpointStore) Get(context.Context, string) (connector.Checkpoint, error) {
	return s.checkpoint, s.err
}
func (managedTestCheckpointStore) Put(context.Context, string, connector.Checkpoint) error {
	return nil
}
func (managedTestCheckpointStore) List(context.Context) ([]connector.FlowCheckpoint, error) {
	return nil, nil
}
func (s managedTestCheckpointStore) GetFenced(context.Context, authority.RunFence) (connector.Checkpoint, error) {
	return s.checkpoint, s.err
}
func (managedTestCheckpointStore) PutFenced(context.Context, authority.RunFence, connector.Checkpoint) error {
	return nil
}
func (managedTestCheckpointStore) PersistCheckpointAndOutboxFenced(context.Context, authority.RunFence, connector.Checkpoint, []connector.OutboxEntry) error {
	return nil
}
func (managedTestCheckpointStore) ListOutboxFenced(context.Context, authority.RunFence) ([]connector.OutboxEntry, error) {
	return nil, nil
}
func (managedTestCheckpointStore) CompleteOutboxFenced(context.Context, authority.RunFence, string, string) error {
	return nil
}

func eventIndex(events []string, want string) int {
	for index, event := range events {
		if event == want {
			return index
		}
	}
	return -1
}

func containsEvent(events []string, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}
