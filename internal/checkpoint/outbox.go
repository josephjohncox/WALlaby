package checkpoint

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

const outboxCodecGobV1 = "gob-v1"

type encodedOutboxEntry struct {
	entry     connector.OutboxEntry
	batchData []byte
	batchHash string
}

func init() {
	// Record Before/After values are interfaces. Register the normalized value
	// shapes emitted by PostgreSQL sources so gob preserves concrete numeric and
	// byte types (JSON decoding would coerce int64 values to float64).
	gob.Register(json.Number(""))
	gob.Register(json.RawMessage{})
	gob.Register(time.Time{})
	gob.Register(map[string]any{})
	gob.Register([]any{})
	gob.Register([]byte{})
	gob.Register([]string{})
	gob.Register([]bool{})
	gob.Register([]int{})
	gob.Register([]int16{})
	gob.Register([]int32{})
	gob.Register([]int64{})
	gob.Register([]uint{})
	gob.Register([]uint16{})
	gob.Register([]uint32{})
	gob.Register([]uint64{})
	gob.Register([]float32{})
	gob.Register([]float64{})
}

func encodeOutboxEntries(flowID string, checkpoint connector.Checkpoint, entries []connector.OutboxEntry) ([]encodedOutboxEntry, error) {
	checkpointID, err := connector.CheckpointPositionID(checkpoint)
	if err != nil {
		return nil, err
	}
	encoded := make([]encodedOutboxEntry, 0, len(entries))
	seen := make(map[string]struct{}, len(entries))
	for _, entry := range entries {
		if entry.FlowID != "" && entry.FlowID != flowID {
			return nil, fmt.Errorf("outbox flow %q does not match transaction flow %q", entry.FlowID, flowID)
		}
		if entry.Destination == "" {
			return nil, fmt.Errorf("outbox destination is required")
		}
		batchID, err := connector.CheckpointPositionID(entry.Batch.Checkpoint)
		if err != nil {
			return nil, fmt.Errorf("outbox destination %s: %w", entry.Destination, err)
		}
		if entry.PositionID != checkpointID || batchID != checkpointID {
			return nil, fmt.Errorf("%w: outbox destination %s position=%q checkpoint=%q batch=%q", connector.ErrCheckpointPosition, entry.Destination, entry.PositionID, checkpointID, batchID)
		}
		identity := entry.Destination + "\x00" + entry.PositionID
		if _, duplicate := seen[identity]; duplicate {
			return nil, fmt.Errorf("duplicate outbox destination %s at position %s", entry.Destination, entry.PositionID)
		}
		seen[identity] = struct{}{}
		entry.Batch.Checkpoint, err = canonicalizeCheckpoint(entry.Batch.Checkpoint)
		if err != nil {
			return nil, fmt.Errorf("canonicalize outbox batch for %s: %w", entry.Destination, err)
		}
		var buffer bytes.Buffer
		if err := gob.NewEncoder(&buffer).Encode(entry.Batch); err != nil {
			return nil, fmt.Errorf("encode outbox batch for %s: %w", entry.Destination, err)
		}
		batchHash, err := hashOutboxBatch(entry.Batch)
		if err != nil {
			return nil, fmt.Errorf("hash outbox batch for %s: %w", entry.Destination, err)
		}
		entry.FlowID = flowID
		if entry.CreatedAt.IsZero() {
			entry.CreatedAt = time.Now().UTC()
		}
		encoded = append(encoded, encodedOutboxEntry{
			entry:     entry,
			batchData: buffer.Bytes(),
			batchHash: batchHash,
		})
	}
	return encoded, nil
}

func decodeOutboxBatch(codec string, batchData []byte) (connector.Batch, error) {
	if codec != outboxCodecGobV1 {
		return connector.Batch{}, fmt.Errorf("unsupported checkpoint outbox codec %q", codec)
	}
	var batch connector.Batch
	if err := gob.NewDecoder(bytes.NewReader(batchData)).Decode(&batch); err != nil {
		return connector.Batch{}, fmt.Errorf("decode outbox batch: %w", err)
	}
	return batch, nil
}
