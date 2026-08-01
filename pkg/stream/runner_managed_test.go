package stream

import (
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestManagedRestoreValidatesAckIntentBeforeFeedbackOrDestinationOpen(t *testing.T) {
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
	if containsEvent(events, "destination.open") {
		t.Fatalf("destination opened before restored feedback validation: %v", events)
	}
}

func TestManagedNewSlotPersistsInitialCutBeforeDestinationOpen(t *testing.T) {
	t.Parallel()
	events := []string{}
	source := &managedTestSource{events: &events, initial: connector.Checkpoint{LSN: "0/20"}}
	coordinator := &managedTestCoordinator{events: &events}
	runner := managedTestRunner(source, &managedTestDestination{events: &events}, coordinator, managedTestCheckpointStore{err: connector.ErrCheckpointNotFound})

	if err := runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"source.open", "coordinator.authorize", "coordinator.validate", "source.ack", "coordinator.receipt", "destination.open"}
	if len(events) < len(wantPrefix) || !reflect.DeepEqual(events[:len(wantPrefix)], wantPrefix) {
		t.Fatalf("managed startup events=%v, want prefix %v", events, wantPrefix)
	}
}

func managedTestRunner(source connector.Source, destination connector.Destination, coordinator ManagedDeliveryCoordinator, checkpoints connector.CheckpointStore) Runner {
	fence := connector.RunFence{
		FlowID: "managed-test", FlowIncarnationID: uuid.New(), Generation: 1,
		AcquisitionID: uuid.New(), ExecutionID: "execution", LeaseEpoch: 1,
	}
	return Runner{
		Source: source,
		SourceSpec: connector.Spec{Options: map[string]string{
			"managed": "true", "source_lineage_id": "lineage-1",
		}},
		Destinations:        []DestinationConfig{{Dest: destination, Spec: connector.Spec{Options: map[string]string{"destination_revision_id": "destination-1"}}}},
		Checkpoints:         checkpoints,
		FlowID:              fence.FlowID,
		AckPolicy:           AckPolicyAll,
		RunFence:            &fence,
		DeliveryCoordinator: coordinator,
	}
}

type managedTestSource struct {
	events  *[]string
	initial connector.Checkpoint
	acks    int
}

func (s *managedTestSource) Open(context.Context, connector.Spec) error {
	*s.events = append(*s.events, "source.open")
	return nil
}
func (s *managedTestSource) Read(context.Context) (connector.Batch, error) {
	return connector.Batch{}, io.EOF
}
func (s *managedTestSource) ReadTransaction(context.Context) (connector.SourceTransaction, error) {
	return connector.SourceTransaction{}, io.EOF
}
func (s *managedTestSource) InitialCheckpoint() (connector.Checkpoint, bool) {
	return s.initial, s.initial.LSN != ""
}
func (s *managedTestSource) Ack(_ context.Context, _ connector.Checkpoint) error {
	s.acks++
	*s.events = append(*s.events, "source.ack")
	return nil
}
func (s *managedTestSource) Close(context.Context) error {
	*s.events = append(*s.events, "source.close")
	return nil
}
func (*managedTestSource) Capabilities() connector.Capabilities {
	return connector.Capabilities{Support: connector.SupportExperimental, SupportsStreaming: true}
}

type managedTestDestination struct{ events *[]string }

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
	return connector.Capabilities{Support: connector.SupportExperimental, Delivery: connector.DeliverySemantics{Declared: true, TransactionalBatch: true, IdempotentReplay: true, ReplaySafe: true}}
}
func (d *managedTestDestination) Close(context.Context) error {
	*d.events = append(*d.events, "destination.close")
	return nil
}
func (*managedTestDestination) Apply(context.Context, connector.DeliveryIntent, connector.Batch) (connector.DeliveryEvidence, error) {
	return connector.DeliveryEvidence{}, nil
}
func (*managedTestDestination) Reconcile(context.Context, connector.DeliveryIntent) (connector.DeliveryDisposition, connector.DeliveryEvidence, error) {
	return connector.DeliveryNotApplied, connector.DeliveryEvidence{}, nil
}

type managedTestCoordinator struct {
	events      *[]string
	validateErr error
}

func (c *managedTestCoordinator) AuthorizeAck(_ context.Context, _ connector.RunFence, checkpoint connector.Checkpoint) (connector.AckGrant, error) {
	*c.events = append(*c.events, "coordinator.authorize")
	position, err := connector.CheckpointPositionID(checkpoint)
	return connector.AckGrant{Checkpoint: checkpoint, PositionID: position}, err
}
func (*managedTestCoordinator) Deliver(context.Context, connector.RunFence, connector.DeliveryIntent, connector.Batch, connector.ManagedDestination) (connector.AckGrant, error) {
	return connector.AckGrant{}, errors.New("unexpected delivery")
}
func (c *managedTestCoordinator) ValidateAckGrant(context.Context, connector.RunFence, connector.AckGrant) error {
	*c.events = append(*c.events, "coordinator.validate")
	return c.validateErr
}
func (c *managedTestCoordinator) RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error {
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

func containsEvent(events []string, want string) bool {
	for _, event := range events {
		if event == want {
			return true
		}
	}
	return false
}
