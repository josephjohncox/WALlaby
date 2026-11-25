package checkpoint

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
	"github.com/josephjohncox/wallaby/pkg/connector"
)

// The model specifies the public atomic checkpoint/outbox contract. Histories
// below call the actual SQLite implementation and encoder, not a fake store.
// This is finite-history linearizability checking, not a crash-durability proof.
type checkpointHistoryInput struct {
	Flow     string
	Kind     string
	Position int
	Value    int
	Dest     string
}

type checkpointHistoryRow struct {
	Destination string
	Position    int
	Value       int
}

type checkpointHistoryOutput struct {
	Error    string
	Position int
	Value    int
	Rows     []checkpointHistoryRow
}

type checkpointHistoryState struct {
	Position int
	Value    int
	Rows     map[string]checkpointHistoryRow
}

func historyRowKey(destination string, position int) string {
	return fmt.Sprintf("%s:%d", destination, position)
}

func sortedHistoryRows(rows map[string]checkpointHistoryRow) []checkpointHistoryRow {
	result := make([]checkpointHistoryRow, 0, len(rows))
	for _, row := range rows {
		result = append(result, row)
	}
	sort.Slice(result, func(i, j int) bool {
		if result[i].Position != result[j].Position {
			return result[i].Position < result[j].Position
		}
		return result[i].Destination < result[j].Destination
	})
	return result
}

func checkpointSequentialModel() porcupine.Model {
	return porcupine.Model{
		Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
			flows := map[string][]porcupine.Operation{}
			for _, operation := range history {
				flow := operation.Input.(checkpointHistoryInput).Flow
				flows[flow] = append(flows[flow], operation)
			}
			keys := make([]string, 0, len(flows))
			for flow := range flows {
				keys = append(keys, flow)
			}
			sort.Strings(keys)
			result := make([][]porcupine.Operation, 0, len(keys))
			for _, flow := range keys {
				result = append(result, flows[flow])
			}
			return result
		},
		Init:  func() interface{} { return checkpointHistoryState{Rows: map[string]checkpointHistoryRow{}} },
		Equal: func(left, right interface{}) bool { return reflect.DeepEqual(left, right) },
		Step: func(state, input, output interface{}) (bool, interface{}) {
			current := state.(checkpointHistoryState)
			op := input.(checkpointHistoryInput)
			observed := output.(checkpointHistoryOutput)
			next := current
			next.Rows = maps.Clone(current.Rows)
			expected := checkpointHistoryOutput{}
			switch op.Kind {
			case "get":
				if current.Position == 0 {
					expected.Error = "not-found"
				} else {
					expected.Position, expected.Value = current.Position, current.Value
				}
			case "list":
				expected.Rows = sortedHistoryRows(current.Rows)
			case "delete":
				key := historyRowKey(op.Dest, op.Position)
				if _, exists := current.Rows[key]; !exists {
					expected.Error = "outbox-not-found"
				} else {
					delete(next.Rows, key)
				}
			case "put", "publish":
				if op.Position < current.Position {
					expected.Error = "regression"
					break
				}
				if op.Kind == "publish" {
					for _, row := range current.Rows {
						if row.Position == op.Position && row.Value != op.Value {
							expected.Error = "conflict"
						}
					}
				}
				if expected.Error != "" {
					break // transaction rollback includes the checkpoint
				}
				next.Position, next.Value = op.Position, op.Value
				if op.Kind == "publish" {
					for _, destination := range []string{"a", "b"} {
						row := checkpointHistoryRow{Destination: destination, Position: op.Position, Value: op.Value}
						next.Rows[historyRowKey(destination, op.Position)] = row
					}
				}
			default:
				return false, current
			}
			return reflect.DeepEqual(expected, observed), next
		},
	}
}

func historyError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, connector.ErrCheckpointNotFound):
		return "not-found"
	case errors.Is(err, connector.ErrCheckpointRegression):
		return "regression"
	case errors.Is(err, connector.ErrOutboxConflict):
		return "conflict"
	case strings.HasSuffix(err.Error(), ": entry not found"):
		// DeleteOutbox reports an exact zero-row deletion with this error;
		// no transport failure is coerced into an effect-free outcome.
		return "outbox-not-found"
	default:
		// Unknown/cancelled writes are NOT recorded as effect-free failures.
		// They fail the test, because this deterministic harness injects none.
		return "unclassified:" + err.Error()
	}
}

func executeCheckpointHistoryOperation(ctx context.Context, store *SQLiteStore, op checkpointHistoryInput) checkpointHistoryOutput {
	checkpoint := connector.Checkpoint{LSN: strconv.Itoa(op.Position), Metadata: map[string]string{"value": strconv.Itoa(op.Value)}}
	var output checkpointHistoryOutput
	var err error
	switch op.Kind {
	case "get":
		var got connector.Checkpoint
		got, err = store.Get(ctx, op.Flow)
		if err == nil {
			output.Position, _ = strconv.Atoi(got.LSN)
			output.Value, _ = strconv.Atoi(got.Metadata["value"])
		}
	case "list":
		var rows []connector.OutboxEntry
		rows, err = store.ListOutbox(ctx, op.Flow)
		if err == nil {
			observed := map[string]checkpointHistoryRow{}
			for _, row := range rows {
				position, _ := strconv.Atoi(row.PositionID)
				value, ok := row.Batch.Records[0].After["value"].(int64)
				if !ok || row.ProjectionFingerprint != "linearizability-v1" || row.FlowID != op.Flow {
					return checkpointHistoryOutput{Error: "invalid persisted outbox row"}
				}
				observed[historyRowKey(row.Destination, position)] = checkpointHistoryRow{Destination: row.Destination, Position: position, Value: int(value)}
			}
			output.Rows = sortedHistoryRows(observed)
		}
	case "delete":
		err = store.DeleteOutbox(ctx, op.Flow, op.Dest, strconv.Itoa(op.Position))
	case "put":
		err = store.Put(ctx, op.Flow, checkpoint)
	case "publish":
		batch := connector.Batch{Checkpoint: checkpoint, Schema: connector.Schema{Name: "events"}, Records: []connector.Record{{Operation: connector.OpInsert, After: map[string]any{"value": int64(op.Value)}}}}
		entries := []connector.OutboxEntry{}
		for _, destination := range []string{"a", "b"} {
			entries = append(entries, connector.OutboxEntry{FlowID: op.Flow, Destination: destination, PositionID: checkpoint.LSN, ProjectionFingerprint: "linearizability-v1", Batch: batch})
		}
		err = store.PersistCheckpointAndOutbox(ctx, op.Flow, checkpoint, entries)
	default:
		return checkpointHistoryOutput{Error: "unknown test operation"}
	}
	output.Error = historyError(err)
	return output
}

func TestSQLiteCheckpointOutboxLinearizability(t *testing.T) {
	for seed := int64(1); seed <= 16; seed++ {
		t.Run(fmt.Sprint(seed), func(t *testing.T) {
			ctx := context.Background()
			store, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "checkpoints.db"))
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = store.Close() })
			var clock atomic.Int64
			var mu sync.Mutex
			history := []porcupine.Operation{}
			coverage := map[string]int{}
			invoke := func(client int, input checkpointHistoryInput) {
				// Logical invocation/return IDs avoid cross-core timestamp/ARM
				// ordering assumptions. Do not put a barrier inside this interval.
				call := clock.Add(1)
				output := executeCheckpointHistoryOperation(ctx, store, input)
				returned := clock.Add(1)
				mu.Lock()
				history = append(history, porcupine.Operation{ClientId: client, Input: input, Call: call, Output: output, Return: returned})
				coverage[input.Kind]++
				coverage[output.Error]++
				mu.Unlock()
			}
			// Non-vacuous witnesses precede the concurrent suffix: duplicate
			// publication, hash conflict, rollback, regression, read and deletion.
			for _, flow := range []string{"one", "two"} {
				for _, input := range []checkpointHistoryInput{
					{Kind: "publish", Position: 1, Value: 1}, {Kind: "publish", Position: 1, Value: 1},
					{Kind: "publish", Position: 1, Value: 2}, {Kind: "publish", Position: 2, Value: 1},
					{Kind: "put", Position: 1, Value: 1}, {Kind: "get"}, {Kind: "list"},
					{Kind: "delete", Position: 1, Dest: "a"},
				} {
					input.Flow = flow
					invoke(0, input)
				}
			}
			random := rand.New(rand.NewSource(seed))
			inputs := make([][]checkpointHistoryInput, 3)
			for client := range inputs {
				for count := 0; count < 6; count++ {
					inputs[client] = append(inputs[client], checkpointHistoryInput{
						Flow: []string{"one", "two"}[random.Intn(2)], Kind: []string{"get", "list", "put", "publish", "delete"}[random.Intn(5)],
						Position: random.Intn(3) + 1, Value: random.Intn(2) + 1, Dest: []string{"a", "b"}[random.Intn(2)],
					})
				}
			}
			start := make(chan struct{})
			var workers sync.WaitGroup
			for client, work := range inputs {
				workers.Go(func() {
					<-start
					for _, input := range work {
						invoke(client+1, input)
					}
				})
			}
			close(start)
			workers.Wait()
			for _, required := range []string{"publish", "get", "list", "delete", "regression", "conflict"} {
				if coverage[required] == 0 {
					t.Fatalf("vacuous history: missing %s", required)
				}
			}
			verdict := porcupine.CheckOperationsTimeout(checkpointSequentialModel(), history, 3*time.Second)
			if verdict != porcupine.Ok {
				// Unknown/timeouts are failures, never proof. Persist the exact
				// synthetic history under the ignored evidence directory.
				dir := filepath.Join("..", "..", ".cache", "protocol")
				_ = os.MkdirAll(dir, 0o700)
				data, _ := json.MarshalIndent(history, "", "  ")
				path := filepath.Join(dir, fmt.Sprintf("checkpoint-history-%d.json", seed))
				_ = os.WriteFile(path, data, 0o600)
				t.Fatalf("checkpoint history is not verified: seed=%d verdict=%s evidence=%s", seed, verdict, path)
			}
		})
	}
}

func TestCheckpointHistoryModelRejectsNonAtomicAndRegressingHistories(t *testing.T) {
	t.Parallel()
	model := checkpointSequentialModel()
	for _, test := range []struct {
		name    string
		history []porcupine.Operation
	}{
		{name: "publication-not-atomic", history: []porcupine.Operation{
			{Input: checkpointHistoryInput{Flow: "one", Kind: "publish", Position: 1, Value: 1}, Call: 0, Output: checkpointHistoryOutput{}, Return: 7},
			{Input: checkpointHistoryInput{Flow: "one", Kind: "get"}, Call: 2, Output: checkpointHistoryOutput{Position: 1, Value: 1}, Return: 3},
			{Input: checkpointHistoryInput{Flow: "one", Kind: "list"}, Call: 4, Output: checkpointHistoryOutput{Rows: []checkpointHistoryRow{}}, Return: 5},
		}},
		{name: "checkpoint-regressed", history: []porcupine.Operation{
			{Input: checkpointHistoryInput{Flow: "one", Kind: "put", Position: 2, Value: 1}, Call: 0, Output: checkpointHistoryOutput{}, Return: 1},
			{Input: checkpointHistoryInput{Flow: "one", Kind: "get"}, Call: 2, Output: checkpointHistoryOutput{Position: 1, Value: 1}, Return: 3},
		}},
	} {
		if verdict := porcupine.CheckOperationsTimeout(model, test.history, time.Second); verdict != porcupine.Illegal {
			t.Errorf("negative control %s was not rejected: %s", test.name, verdict)
		}
	}
}
