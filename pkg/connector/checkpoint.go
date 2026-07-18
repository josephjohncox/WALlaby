package connector

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

type checkpointPositionKind uint8

const (
	checkpointPositionOrdinal checkpointPositionKind = iota + 1
	checkpointPositionPostgresLSN
)

// CanonicalizeCheckpointPosition validates and canonicalizes a PostgreSQL LSN
// (HEX/HEX) or non-negative decimal ordinal. Surrounding whitespace and signed
// ordinals are rejected. PostgreSQL LSN hex digits are accepted in either case
// and stored uppercase; decimal leading zeroes are removed.
func CanonicalizeCheckpointPosition(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("%w: position is empty", ErrCheckpointPosition)
	}
	if strings.TrimSpace(raw) != raw {
		return "", fmt.Errorf("%w: position must not contain surrounding whitespace: %q", ErrCheckpointPosition, raw)
	}
	if strings.Count(raw, "/") == 1 {
		parts := strings.SplitN(raw, "/", 2)
		if parts[0] == "" || parts[1] == "" {
			return "", fmt.Errorf("%w: malformed PostgreSQL LSN %q", ErrCheckpointPosition, raw)
		}
		high, err := strconv.ParseUint(parts[0], 16, 32)
		if err != nil {
			return "", fmt.Errorf("%w: parse PostgreSQL LSN %q: %w", ErrCheckpointPosition, raw, err)
		}
		low, err := strconv.ParseUint(parts[1], 16, 32)
		if err != nil {
			return "", fmt.Errorf("%w: parse PostgreSQL LSN %q: %w", ErrCheckpointPosition, raw, err)
		}
		return strings.ToUpper(strconv.FormatUint(high, 16) + "/" + strconv.FormatUint(low, 16)), nil
	}
	if strings.Contains(raw, "/") || raw[0] == '+' || raw[0] == '-' {
		return "", fmt.Errorf("%w: expected PostgreSQL LSN or unsigned decimal ordinal, got %q", ErrCheckpointPosition, raw)
	}
	for _, digit := range raw {
		if digit < '0' || digit > '9' {
			return "", fmt.Errorf("%w: expected PostgreSQL LSN or unsigned decimal ordinal, got %q", ErrCheckpointPosition, raw)
		}
	}
	value, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		return "", fmt.Errorf("%w: expected PostgreSQL LSN or unsigned decimal ordinal, got %q", ErrCheckpointPosition, raw)
	}
	return value.String(), nil
}

// CheckpointPositionID returns the deterministic identity used by traces and
// the durable fan-out outbox. Stream positions use their canonical LSN or
// ordinal. Empty-LSN control/backfill positions hash every metadata key/value
// in stable key order, so cursor and completion changes remain distinct across
// process restarts.
func CheckpointPositionID(checkpoint Checkpoint) (string, error) {
	if checkpoint.LSN != "" {
		return CanonicalizeCheckpointPosition(checkpoint.LSN)
	}
	if len(checkpoint.Metadata) == 0 {
		return "", fmt.Errorf("%w: empty checkpoint metadata", ErrCheckpointPosition)
	}
	keys := make([]string, 0, len(checkpoint.Metadata))
	for key := range checkpoint.Metadata {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	hash := sha256.New()
	var size [8]byte
	for _, key := range keys {
		value := checkpoint.Metadata[key]
		binary.BigEndian.PutUint64(size[:], uint64(len(key)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(key))
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}
	return "checkpoint:" + hex.EncodeToString(hash.Sum(nil)), nil
}

// CompareCheckpointLSN compares canonical PostgreSQL LSNs or decimal batch
// ordinals. Positions of different kinds are intentionally incomparable.
func CompareCheckpointLSN(left, right string) (int, error) {
	leftValue, leftKind, err := parseCheckpointPosition(left)
	if err != nil {
		return 0, err
	}
	rightValue, rightKind, err := parseCheckpointPosition(right)
	if err != nil {
		return 0, err
	}
	if leftKind != rightKind {
		return 0, fmt.Errorf("%w: cannot compare %q with %q", ErrCheckpointPosition, left, right)
	}
	return leftValue.Cmp(rightValue), nil
}

func parseCheckpointPosition(raw string) (*big.Int, checkpointPositionKind, error) {
	position, err := CanonicalizeCheckpointPosition(raw)
	if err != nil {
		return nil, 0, err
	}
	if strings.Contains(position, "/") {
		parts := strings.SplitN(position, "/", 2)
		high, _ := strconv.ParseUint(parts[0], 16, 32)
		low, _ := strconv.ParseUint(parts[1], 16, 32)
		return new(big.Int).SetUint64(high<<32 | low), checkpointPositionPostgresLSN, nil
	}
	value, _ := new(big.Int).SetString(position, 10)
	return value, checkpointPositionOrdinal, nil
}
