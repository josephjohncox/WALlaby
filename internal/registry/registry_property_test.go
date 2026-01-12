package registry

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/josephjohncox/wallaby/internal/schema"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"pgregory.net/rapid"
)

type memoryStore struct {
	events []DDLEvent
	nextID int64
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
			err := MarkDDLAppliedByLSN(context.Background(), store, "", lsn)
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
				if err != nil {
					t.Fatalf("expected rejected ddl to remain rejected")
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
