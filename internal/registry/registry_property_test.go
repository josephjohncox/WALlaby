package registry

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

type memoryStore struct {
	events    []DDLEvent
	nextID    int64
	attempts  map[int64]map[string]struct{}
	receipts  map[int64]map[string]struct{}
	manifests map[int64][]string
}

func (m *memoryStore) RegisterSchema(context.Context, connector.Schema) error { return nil }

func (m *memoryStore) RecordDDL(_ context.Context, flowID string, ddl string, plan schema.Plan, lsn string, status string) (int64, error) {
	if status == "" {
		status = StatusPending
	}
	m.nextID++
	event := DDLEvent{
		ID:        m.nextID,
		FlowID:    flowID,
		DDL:       ddl,
		Plan:      plan,
		LSN:       lsn,
		Status:    status,
		CreatedAt: time.Unix(0, m.nextID),
	}
	m.events = append(m.events, event)
	return event.ID, nil
}

func (m *memoryStore) SetDDLStatus(_ context.Context, id int64, status string) error {
	if status == StatusApplied {
		return ErrExecutionReceiptRequired
	}
	for i := range m.events {
		if m.events[i].ID != id {
			continue
		}
		m.events[i].Status = status
		if status == StatusApplied {
			m.events[i].AppliedAt = time.Unix(0, time.Now().UnixNano())
		} else {
			m.events[i].AppliedAt = time.Time{}
		}
		break
	}
	return nil
}

func (m *memoryStore) PrepareDDLExecution(_ context.Context, flowID, lsn, destination string, expectedDestinations []string) (connector.DDLExecutionState, error) {
	event, err := m.GetDDLByLSN(context.Background(), flowID, lsn)
	if err != nil {
		return connector.DDLExecutionUnknown, err
	}
	if event.Status != StatusApproved && event.Status != StatusApplied {
		return connector.DDLExecutionUnknown, &connector.DDLGateError{FlowID: flowID, LSN: lsn, Status: event.Status, EventID: event.ID}
	}
	expected := normalizedDestinations(expectedDestinations)
	if manifest := m.manifests[event.ID]; manifest != nil && !equalDestinations(manifest, expected) {
		return connector.DDLExecutionUnknown, ErrExecutionManifestChanged
	}
	if m.manifests == nil {
		m.manifests = make(map[int64][]string)
	}
	m.manifests[event.ID] = expected
	if _, ok := m.receipts[event.ID][destination]; ok {
		return connector.DDLExecutionComplete, nil
	}
	if _, ok := m.attempts[event.ID][destination]; ok {
		return connector.DDLExecutionRetry, nil
	}
	if m.attempts == nil {
		m.attempts = make(map[int64]map[string]struct{})
	}
	if m.attempts[event.ID] == nil {
		m.attempts[event.ID] = make(map[string]struct{})
	}
	m.attempts[event.ID][destination] = struct{}{}
	return connector.DDLExecutionNew, nil
}

func (m *memoryStore) RecordVacuousDDLExecution(_ context.Context, flowID, lsn, _ string) error {
	for i := range m.events {
		if m.events[i].FlowID == flowID && m.events[i].LSN == lsn {
			if m.events[i].Status != StatusApproved && m.events[i].Status != StatusApplied {
				return &connector.DDLGateError{Status: m.events[i].Status}
			}
			m.events[i].Status = StatusApplied
			return nil
		}
	}
	return errors.New("ddl event not found")
}

func (m *memoryStore) RecordDDLExecution(
	_ context.Context,
	flowID, lsn, _ string, destination string,
	expectedDestinations []string,
) error {
	event, err := m.GetDDLByLSN(context.Background(), flowID, lsn)
	if err != nil {
		return err
	}
	if event.Status != StatusApproved && event.Status != StatusApplied {
		return &connector.DDLGateError{FlowID: flowID, LSN: lsn, Status: event.Status, EventID: event.ID}
	}
	expected := normalizedDestinations(expectedDestinations)
	if manifest := m.manifests[event.ID]; manifest != nil && !equalDestinations(manifest, expected) {
		return errors.New("DDL execution destination manifest changed during replay")
	}
	if m.manifests == nil {
		m.manifests = make(map[int64][]string)
	}
	m.manifests[event.ID] = expected
	if _, ok := m.attempts[event.ID][destination]; !ok {
		return ErrDDLExecutionNotPrepared
	}
	if m.receipts == nil {
		m.receipts = make(map[int64]map[string]struct{})
	}
	if m.receipts[event.ID] == nil {
		m.receipts[event.ID] = make(map[string]struct{})
	}
	m.receipts[event.ID][destination] = struct{}{}
	if len(m.receipts[event.ID]) == len(expected) {
		for index := range m.events {
			if m.events[index].ID == event.ID {
				m.events[index].Status = StatusApplied
				m.events[index].AppliedAt = time.Now()
			}
		}
	}
	return nil
}

func (m *memoryStore) ListPendingDDL(_ context.Context, flowID string) ([]DDLEvent, error) {
	items := []DDLEvent{}
	for _, event := range m.events {
		if flowID != "" && event.FlowID != flowID {
			continue
		}
		if event.Status == StatusPending {
			items = append(items, event)
		}
	}
	return items, nil
}

func (m *memoryStore) GetDDL(_ context.Context, id int64) (DDLEvent, error) {
	for _, event := range m.events {
		if event.ID == id {
			return event, nil
		}
	}
	return DDLEvent{}, ErrNotFound
}

func (m *memoryStore) GetDDLByLSN(_ context.Context, flowID string, lsn string) (DDLEvent, error) {
	for i := len(m.events) - 1; i >= 0; i-- {
		if m.events[i].LSN == lsn && (flowID == "" || m.events[i].FlowID == flowID) {
			return m.events[i], nil
		}
	}
	return DDLEvent{}, ErrNotFound
}

func (m *memoryStore) ListDDL(_ context.Context, flowID string, status string) ([]DDLEvent, error) {
	items := []DDLEvent{}
	for _, event := range m.events {
		if flowID != "" && event.FlowID != flowID {
			continue
		}
		if status == "" || status == "all" || event.Status == status {
			items = append(items, event)
		}
	}
	return items, nil
}

func TestRegistryAppliedImpliesApprovedRapid(t *testing.T) {
	rapid.Check(t, func(t *rapid.T) {
		store := &memoryStore{}
		count := rapid.IntRange(1, 10).Draw(t, "count")
		base := uint64(rapid.IntRange(1, 1_000_000).Draw(t, "base"))
		statuses := make(map[string]string, count)

		for i := 0; i < count; i++ {
			lsn := pglogrepl.LSN(base + uint64(i)).String()
			status := rapid.SampledFrom([]string{StatusPending, StatusApproved, StatusRejected}).Draw(t, "status")
			statuses[lsn] = status
			if _, err := store.RecordDDL(context.Background(), "", "ALTER TABLE events ADD COLUMN col text", schema.Plan{}, lsn, status); err != nil {
				t.Fatalf("record ddl: %v", err)
			}
		}

		for lsn, status := range statuses {
			if !rapid.Bool().Draw(t, "apply") {
				continue
			}
			_, err := PrepareDDLExecution(context.Background(), store, "", lsn, "destination", []string{"destination"})
			if err == nil {
				err = RecordDDLExecution(context.Background(), store, "", lsn, "ddl", "destination", []string{"destination"})
			}
			switch status {
			case StatusApproved:
				if err != nil {
					t.Fatalf("expected apply to succeed")
				}
			case StatusPending:
				if err == nil {
					t.Fatalf("expected apply to require approval")
				}
			case StatusRejected:
				if err == nil {
					t.Fatalf("expected rejected ddl execution to fail")
				}
			}
		}

		events, err := store.ListDDL(context.Background(), "", "")
		if err != nil {
			t.Fatalf("list ddl: %v", err)
		}

		var prevLSN uint64
		for i, event := range events {
			lsn := parseLSN(event.LSN)
			if i > 0 && lsn < prevLSN {
				t.Fatalf("lsn ordering not monotonic")
			}
			prevLSN = lsn
			if event.Status == StatusApplied && event.AppliedAt.IsZero() {
				t.Fatalf("applied ddl missing timestamp")
			}
			if event.Status == StatusApplied && statuses[event.LSN] != StatusApproved {
				t.Fatalf("applied ddl without approval")
			}
		}
	})
}

func parseLSN(value string) uint64 {
	if value == "" {
		return 0
	}
	lsn, err := pglogrepl.ParseLSN(value)
	if err != nil {
		return 0
	}
	return uint64(lsn)
}
