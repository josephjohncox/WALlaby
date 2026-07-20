package stream

import (
	"context"
	"fmt"
	"sync"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

type testDDLReceiptStore struct {
	mu           sync.Mutex
	lockMu       sync.Mutex
	locks        map[string]*sync.Mutex
	beforeLock   func(flowID, destination string)
	attempts     map[string]struct{}
	receipts     map[string]struct{}
	onPrepare    func(flowID, lsn, destination string, expected []string) (connector.DDLExecutionState, error)
	beforeRecord func(flowID, lsn, ddl, destination string, expected []string) error
	onRecord     func(flowID, lsn, ddl, destination string, expected []string) error
}

func (s *testDDLReceiptStore) WithDDLExecutionLock(_ context.Context, flowID, destination string, fn func() error) error {
	key := flowID + "\x00" + destination
	s.lockMu.Lock()
	if s.locks == nil {
		s.locks = make(map[string]*sync.Mutex)
	}
	lock := s.locks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		s.locks[key] = lock
	}
	s.lockMu.Unlock()
	if s.beforeLock != nil {
		s.beforeLock(flowID, destination)
	}
	lock.Lock()
	defer lock.Unlock()
	return fn()
}

func (s *testDDLReceiptStore) PrepareDDLExecution(_ context.Context, flowID, lsn, destination string, expected []string) (connector.DDLExecutionState, error) {
	if s.onPrepare != nil {
		return s.onPrepare(flowID, lsn, destination, expected)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	key := ddlReceiptTestKey(flowID, lsn, destination)
	if _, ok := s.receipts[key]; ok {
		return connector.DDLExecutionComplete, nil
	}
	if _, ok := s.attempts[key]; ok {
		return connector.DDLExecutionRetry, nil
	}
	if s.attempts == nil {
		s.attempts = make(map[string]struct{})
	}
	s.attempts[key] = struct{}{}
	return connector.DDLExecutionNew, nil
}

func (s *testDDLReceiptStore) RecordDDLExecution(
	_ context.Context,
	flowID, lsn, ddl, destination string,
	expected []string,
) error {
	if s.beforeRecord != nil {
		if err := s.beforeRecord(flowID, lsn, ddl, destination, expected); err != nil {
			return err
		}
	}
	s.mu.Lock()
	key := ddlReceiptTestKey(flowID, lsn, destination)
	if _, ok := s.attempts[key]; !ok {
		s.mu.Unlock()
		return fmt.Errorf("receipt recorded without a prepared attempt")
	}
	if s.receipts == nil {
		s.receipts = make(map[string]struct{})
	}
	s.receipts[key] = struct{}{}
	s.mu.Unlock()
	if s.onRecord != nil {
		return s.onRecord(flowID, lsn, ddl, destination, expected)
	}
	return nil
}

func (s *testDDLReceiptStore) allReceipted(flowID, lsn string, destinations []string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, destination := range destinations {
		if _, ok := s.receipts[ddlReceiptTestKey(flowID, lsn, destination)]; !ok {
			return false
		}
	}
	return true
}

func ddlReceiptTestKey(flowID, lsn, destination string) string {
	return fmt.Sprintf("%s\x00%s\x00%s", flowID, lsn, destination)
}
