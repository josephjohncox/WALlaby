package stream

import (
	"context"
	"fmt"
	"sync"
)

type testDDLReceiptStore struct {
	mu        sync.Mutex
	receipts  map[string]struct{}
	onPrepare func(flowID, lsn, destination string, expected []string) (bool, error)
	onRecord  func(flowID, lsn, ddl, destination string, expected []string) error
}

func (s *testDDLReceiptStore) PrepareDDLExecution(_ context.Context, flowID, lsn, destination string, expected []string) (bool, error) {
	if s.onPrepare != nil {
		return s.onPrepare(flowID, lsn, destination, expected)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.receipts[ddlReceiptTestKey(flowID, lsn, destination)]
	return ok, nil
}

func (s *testDDLReceiptStore) RecordDDLExecution(
	_ context.Context,
	flowID, lsn, ddl, destination string,
	expected []string,
) error {
	s.mu.Lock()
	if s.receipts == nil {
		s.receipts = make(map[string]struct{})
	}
	s.receipts[ddlReceiptTestKey(flowID, lsn, destination)] = struct{}{}
	s.mu.Unlock()
	if s.onRecord != nil {
		return s.onRecord(flowID, lsn, ddl, destination, expected)
	}
	return nil
}

func ddlReceiptTestKey(flowID, lsn, destination string) string {
	return fmt.Sprintf("%s\x00%s\x00%s", flowID, lsn, destination)
}
