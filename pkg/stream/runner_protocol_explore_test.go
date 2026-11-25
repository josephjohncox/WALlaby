package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/josephjohncox/wallaby/internal/authority"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// This explores the production Runner, not a reimplementation of its loop.
// The authority, ordered source and marker-atomic target below are explicit
// assume-guarantee contracts, NOT proofs of PostgreSQL or destination drivers.
const managedProtocolScheduleCount = 1315

var (
	errProtocolDefinite = errors.New("protocol scheduled definite rejection")
	errProtocolCrash    = errors.New("protocol scheduled worker crash")
)

type protocolFault struct {
	position int
	point    string
	kind     string
}

type protocolModel struct {
	plan          []protocolFault
	fired         map[int]bool
	counts        map[string]int
	fence         connector.RunFence
	checkpoint    int
	flushed       int
	settled       [4]bool
	markers       [4]int
	intents       map[int]connector.DeliveryIntent
	retryPosition int
	stop          bool
	violation     string
	cancel        context.CancelFunc
	badModel      string
}

func (m *protocolModel) reject(property, detail string) error {
	if m.violation == "" {
		m.violation = property + ": " + detail
	}
	m.cancel()
	return errors.New(m.violation)
}

func protocolLSN(position int) string { return fmt.Sprintf("0/%X", position*16) }

func protocolPosition(checkpoint connector.Checkpoint) int {
	for position := 1; position <= 3; position++ {
		if checkpoint.LSN == protocolLSN(position) {
			return position
		}
	}
	return 0
}

func (m *protocolModel) choice(ctx context.Context, position int, point string) (string, error) {
	if m.stop {
		return "", m.reject("P4", "operation continued after a terminal error")
	}
	if err := ctx.Err(); err != nil {
		return "", m.reject("P7", "operation started after cancellation")
	}
	for index, fault := range m.plan {
		if m.fired[index] || fault.position != position || fault.point != point {
			continue
		}
		m.fired[index] = true
		m.counts[fault.kind]++
		switch fault.kind {
		case "definite":
			m.stop = true
			return "", errProtocolDefinite
		case "cancel":
			m.stop = true
			m.cancel()
			return "", context.Canceled
		case "takeover":
			m.fence.LeaseEpoch++
			if m.badModel != "stale-authority" {
				m.stop = true
				m.counts["stale-rejected"]++
				return "", authority.ErrFenceRejected
			}
			return "", nil
		case "unknown-before":
			if point == "dispatch" && position != 2 {
				m.retryPosition = position
			} else {
				m.stop = true
			}
			return "", connector.ErrDeliveryIndeterminate
		default:
			return fault.kind, nil
		}
	}
	return "", nil
}

func (m *protocolModel) after(position int, point, fault string) error {
	switch fault {
	case "unknown-after":
		if point == "dispatch" && position != 2 {
			m.retryPosition = position
		} else {
			m.stop = true
		}
		return connector.ErrDeliveryIndeterminate
	case "crash-after":
		m.stop = true
		return errProtocolCrash
	default:
		return nil
	}
}

func (m *protocolModel) checkFence(fence connector.RunFence) error {
	// Independent monitor: even a deliberately broken authority oracle cannot
	// turn a stale mutation into passing evidence for the Runner theorem.
	if fence != m.fence {
		return m.reject("A-FENCE", "contract admitted a stale producer")
	}
	return nil
}

func (m *protocolModel) settle(position int) error {
	if position < 1 || position > 3 {
		return m.reject("P2", "unknown checkpoint grant")
	}
	for previous := 1; previous < position; previous++ {
		if !m.settled[previous] {
			return m.reject("A-SRC-ORDER", "source skipped an unsettled transaction")
		}
	}
	m.settled[position] = true
	if position > m.checkpoint {
		m.checkpoint = position
	}
	return nil
}

func (m *protocolModel) grant() connector.AckGrant {
	checkpoint := connector.Checkpoint{LSN: protocolLSN(m.checkpoint)}
	position, _ := connector.CheckpointPositionID(checkpoint)
	return connector.AckGrant{Checkpoint: checkpoint, PositionID: position}
}

func (m *protocolModel) AuthorizeAck(ctx context.Context, fence connector.RunFence, checkpoint connector.Checkpoint, _ connector.ManagedSchemaBaselinePayload) (connector.AckGrant, error) {
	position := protocolPosition(checkpoint)
	if checkpoint.LSN == "0/0" && m.checkpoint == 0 {
		if err := m.checkFence(fence); err != nil {
			return connector.AckGrant{}, err
		}
		m.settled[0] = true // explicit, already-validated initial slot cut
		return m.grant(), nil
	}
	if position != 2 {
		return connector.AckGrant{}, m.reject("P5", "data was authorized without destination delivery")
	}
	fault, err := m.choice(ctx, position, "dispatch")
	if err != nil {
		return connector.AckGrant{}, err
	}
	if err := m.checkFence(fence); err != nil {
		return connector.AckGrant{}, err
	}
	m.counts["filtered"]++
	if err := m.settle(position); err != nil {
		return connector.AckGrant{}, err
	}
	return m.grant(), m.after(position, "dispatch", fault)
}

func (m *protocolModel) DeliverTransaction(ctx context.Context, fence connector.RunFence, intent connector.DeliveryIntent, transaction connector.SourceTransaction, _ connector.ManagedSchemaBaselinePayload, destination connector.ManagedTransactionDestination) (connector.AckGrant, error) {
	position := protocolPosition(transaction.Checkpoint)
	if position == 2 || len(transaction.Fragments) == 0 {
		return connector.AckGrant{}, m.reject("P5", "filtered transaction reached the destination")
	}
	if transaction.SourceLineageID != "lineage-1" {
		return connector.AckGrant{}, m.reject("P6", "wrong lineage reached the coordinator")
	}
	hash, logical, err := connector.SourceTransactionIdentity(transaction)
	if err != nil {
		return connector.AckGrant{}, m.reject("P6", "invalid transaction identity")
	}
	positionID, _ := connector.CheckpointPositionID(transaction.Checkpoint)
	if intent.FlowID != fence.FlowID || intent.FlowIncarnationID != fence.FlowIncarnationID.String() || intent.Generation != fence.Generation || intent.AcquisitionID != fence.AcquisitionID.String() || intent.LeaseEpoch != fence.LeaseEpoch || intent.PositionID != positionID || intent.ContentHash != hash || intent.LogicalBatchID != logical || intent.SourceLineageID != "lineage-1" || intent.DestinationRevisionID != "destination-1" {
		return connector.AckGrant{}, m.reject("P6", "intent is not bound to the transaction and fence")
	}
	if previous, ok := m.intents[position]; ok && previous != intent {
		return connector.AckGrant{}, m.reject("P3", "retry changed immutable delivery identity")
	}
	m.intents[position] = intent
	if m.retryPosition != 0 {
		if m.retryPosition != position {
			return connector.AckGrant{}, m.reject("P3", "another transaction overtook an uncertain delivery")
		}
		m.counts["retried"]++
		m.retryPosition = 0
	}
	fault, err := m.choice(ctx, position, "dispatch")
	if err != nil {
		return connector.AckGrant{}, err
	}
	if err := m.checkFence(fence); err != nil {
		return connector.AckGrant{}, err
	}
	if !m.settled[position] {
		if _, err := destination.ApplyTransaction(ctx, intent, transaction); err != nil {
			return connector.AckGrant{}, err
		}
		if err := m.settle(position); err != nil {
			return connector.AckGrant{}, err
		}
	}
	return m.grant(), m.after(position, "dispatch", fault)
}

func (m *protocolModel) ValidateAckGrant(_ context.Context, fence connector.RunFence, grant connector.AckGrant) error {
	if err := m.checkFence(fence); err != nil {
		return err
	}
	if m.retryPosition != 0 || !reflect.DeepEqual(grant, m.grant()) || !m.settled[m.checkpoint] {
		return m.reject("P2/P3", "feedback is not the current committed grant")
	}
	return nil
}

func (*protocolModel) RecordAckReceipt(context.Context, connector.RunFence, connector.AckGrant, string) error {
	return errors.New("unexpected bypass of combined source-feedback contract")
}

func (m *protocolModel) CommitSourceFeedback(ctx context.Context, fence connector.RunFence, grant connector.AckGrant, source connector.FlushEvidenceSource) error {
	position := protocolPosition(grant.Checkpoint)
	if err := m.ValidateAckGrant(ctx, fence, grant); err != nil {
		return err
	}
	fault, err := m.choice(ctx, position, "feedback")
	if err != nil {
		return err
	}
	if err := m.checkFence(fence); err != nil {
		return err
	}
	if _, err := source.AckWithEvidence(ctx, grant.Checkpoint); err != nil {
		return err
	}
	return m.after(position, "feedback", fault)
}

type protocolSource struct {
	managedTestSource
	model *protocolModel
	start int
}

func (s *protocolSource) ReadTransaction(ctx context.Context) (connector.SourceTransaction, error) {
	position := s.start + s.transactionRead + 1
	if position > 3 {
		return connector.SourceTransaction{}, io.EOF
	}
	if s.model.retryPosition != 0 || s.model.flushed < position-1 {
		return connector.SourceTransaction{}, s.model.reject("P1/P3", "read preceded prior feedback")
	}
	fault, err := s.model.choice(ctx, position, "read")
	if err != nil {
		return connector.SourceTransaction{}, err
	}
	transaction, err := s.managedTestSource.ReadTransaction(ctx)
	if err != nil {
		return transaction, err
	}
	return transaction, s.model.after(position, "read", fault)
}

func (s *protocolSource) AckWithEvidence(ctx context.Context, checkpoint connector.Checkpoint) (connector.SourceFlushEvidence, error) {
	position := protocolPosition(checkpoint)
	if position != s.model.checkpoint || !s.model.settled[position] {
		return connector.SourceFlushEvidence{}, s.model.reject("P2", "source flush has no committed authorization")
	}
	if err := ctx.Err(); err != nil {
		return connector.SourceFlushEvidence{}, s.model.reject("P7", "source flush after cancellation")
	}
	if position > s.model.flushed {
		s.model.flushed = position
	}
	return connector.SourceFlushEvidence{ObservedFlushLSN: checkpoint.LSN}, nil
}

type protocolDestination struct {
	managedTestDestination
	model *protocolModel
}

func (d *protocolDestination) ApplyTransaction(_ context.Context, intent connector.DeliveryIntent, transaction connector.SourceTransaction) (connector.DeliveryEvidence, error) {
	position := protocolPosition(transaction.Checkpoint)
	if d.model.markers[position] == 0 {
		d.model.markers[position]++
	}
	if d.model.badModel == "duplicate-target" {
		d.model.markers[position]++
	}
	return connector.DeliveryEvidence{ExternalID: fmt.Sprint(position), ContentHash: intent.ContentHash}, nil
}

type protocolProjector struct{ managedBootstrapTestProjector }

func (protocolProjector) ProjectTransaction(transaction connector.SourceTransaction) (connector.SourceTransaction, ProjectionDecision, error) {
	if transaction.TransactionID == 2 {
		transaction.Fragments = nil
		return transaction, ProjectionFiltered, nil
	}
	return transaction, ProjectionIncluded, nil
}

func protocolTransactions() []connector.SourceTransaction {
	transactions := make([]connector.SourceTransaction, 3)
	for index := range transactions {
		position := index + 1
		lsn := protocolLSN(position)
		transaction := connector.SourceTransaction{SourceLineageID: "lineage-1", TransactionID: uint32(position), BeginLSN: lsn, CommitLSN: lsn, EndLSN: lsn, Checkpoint: connector.Checkpoint{LSN: lsn}}
		fragments := 1
		if position == 3 {
			fragments = 2
		}
		for ordinal := 0; ordinal < fragments; ordinal++ {
			transaction.Fragments = append(transaction.Fragments, connector.TransactionFragment{Ordinal: uint64(ordinal), Batch: connector.Batch{
				Schema:  connector.Schema{Name: "widgets", Namespace: "public", Columns: []connector.Column{{Name: "id", Type: "int8"}}},
				Records: []connector.Record{{Table: "widgets", Operation: connector.OpInsert, After: map[string]any{"id": int64(position*10 + ordinal)}}},
			}})
		}
		transactions[index] = transaction
	}
	return transactions
}

func runProtocolSchedule(plan []protocolFault, badModel string) (*protocolModel, error) {
	model := &protocolModel{plan: plan, fired: map[int]bool{}, counts: map[string]int{}, intents: map[int]connector.DeliveryIntent{}, badModel: badModel}
	transactions := protocolTransactions()
	if badModel == "skipping-source" {
		transactions = transactions[1:]
	}
	if badModel == "foreign-lineage" {
		transactions[0].SourceLineageID = "other-lineage"
	}
	var runErr error
	for restart := 0; restart <= 1; restart++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		model.cancel = cancel
		model.stop = false
		events := []string{}
		source := &protocolSource{managedTestSource: managedTestSource{events: &events, transactions: transactions[model.checkpoint:]}, model: model, start: model.checkpoint}
		destination := &protocolDestination{managedTestDestination: managedTestDestination{events: &events}, model: model}
		source.initial = connector.Checkpoint{LSN: "0/0"}
		checkpoint := connector.Checkpoint{}
		checkpointErr := connector.ErrCheckpointNotFound
		if model.checkpoint > 0 {
			checkpoint = connector.Checkpoint{LSN: protocolLSN(model.checkpoint)}
			source.initial = checkpoint
			checkpointErr = nil
		}
		runner := managedTestRunner(source, destination, model, managedTestCheckpointStore{checkpoint: checkpoint, err: checkpointErr})
		runner.Destinations[0].Projector = protocolProjector{}
		if restart == 0 {
			model.fence = *runner.RunFence
		} else {
			model.fence.LeaseEpoch++
			model.fence.AcquisitionID = uuid.New()
			model.fence.ExecutionID = "replacement"
			model.counts["restarted"]++
			*runner.RunFence = model.fence
		}
		runErr = runner.Run(ctx)
		cancel()
		if !errors.Is(runErr, errProtocolCrash) {
			break
		}
	}
	if badModel == "foreign-lineage" {
		if runErr == nil || !strings.Contains(runErr.Error(), "does not match configured") || len(model.intents) != 0 || model.checkpoint != 0 {
			model.violation = "P6: foreign lineage was not rejected before delivery"
		}
		return model, runErr
	}
	if runErr == nil && model.stop {
		model.violation = "P4: terminal error was swallowed"
	}
	if runErr == nil && (model.checkpoint != 3 || model.flushed != 3 || !model.settled[1] || !model.settled[2] || !model.settled[3]) {
		model.violation = "P5/P8: successful run did not settle and flush every transaction"
	}
	for _, position := range []int{1, 3} {
		if model.settled[position] && model.markers[position] != 1 {
			model.violation = "P8: target marker is missing or duplicated"
		}
	}
	if model.violation == "" && runErr != nil && !model.stop {
		model.violation = "P3/P5: unexpected failure outside the scheduled terminal contract: " + runErr.Error()
	}
	return model, runErr
}

func TestManagedProtocolExplore(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		kinds := []string{"definite", "unknown-before", "unknown-after", "crash-after", "takeover", "cancel"}
		sites := []protocolFault{}
		for position := 1; position <= 3; position++ {
			for _, point := range []string{"read", "dispatch", "feedback"} {
				sites = append(sites, protocolFault{position: position, point: point})
			}
		}
		plans := [][]protocolFault{nil}
		for i, first := range sites {
			for _, kind := range kinds {
				first.kind = kind
				plans = append(plans, []protocolFault{first})
				for _, second := range sites[i+1:] {
					for _, nextKind := range kinds {
						if kind == "crash-after" && nextKind == "crash-after" {
							continue
						}
						second.kind = nextKind
						plans = append(plans, []protocolFault{first, second})
					}
				}
			}
		}
		if len(plans) != managedProtocolScheduleCount {
			t.Fatalf("schedule coverage changed: got %d, want %d", len(plans), managedProtocolScheduleCount)
		}
		counts := map[string]int{}
		for index, plan := range plans {
			model, err := runProtocolSchedule(plan, "")
			if model.violation != "" {
				t.Fatalf("schedule=%d plan=%+v violation=%s err=%v", index, plan, model.violation, err)
			}
			for key, value := range model.counts {
				counts[key] += value
			}
		}
		for _, required := range append(kinds, "filtered", "retried", "restarted", "stale-rejected") {
			if counts[required] == 0 {
				t.Fatalf("vacuous protocol exploration: %s never executed", required)
			}
		}
		t.Logf("managed protocol schedules=%d (up to two faults; at most one restart), coverage=%v; assumptions: ordered source, fenced atomic coordinator, marker-atomic target", len(plans), counts)
	})
}

func TestManagedProtocolRejectsForeignLineage(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		model, err := runProtocolSchedule(nil, "foreign-lineage")
		if model.violation != "" || err == nil {
			t.Fatalf("P6: lineage validation failed: violation=%q err=%v", model.violation, err)
		}
	})
}

func TestManagedProtocolModelNegativeControls(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		for _, test := range []struct {
			model    string
			plan     []protocolFault
			property string
		}{
			{model: "stale-authority", plan: []protocolFault{{position: 1, point: "dispatch", kind: "takeover"}}, property: "A-FENCE"},
			{model: "skipping-source", property: "A-SRC-ORDER"},
			{model: "duplicate-target", property: "P8"},
		} {
			model, _ := runProtocolSchedule(test.plan, test.model)
			if !strings.Contains(model.violation, test.property) {
				t.Fatalf("negative control %s survived: violation=%q", test.model, model.violation)
			}
		}
	})
}
