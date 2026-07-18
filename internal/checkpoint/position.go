package checkpoint

import (
	"fmt"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

// canonicalizeCheckpoint rejects malformed stream positions before they
// reach a store. Empty LSNs are reserved for explicitly identified control
// positions; ordinary stream checkpoints must always carry a valid LSN or
// decimal ordinal.
func canonicalizeCheckpoint(checkpoint connector.Checkpoint) (connector.Checkpoint, error) {
	if checkpoint.LSN != "" {
		position, err := connector.CanonicalizeCheckpointPosition(checkpoint.LSN)
		if err != nil {
			return connector.Checkpoint{}, err
		}
		checkpoint.LSN = position
		return checkpoint, nil
	}
	if isSupportedControlPosition(checkpoint.Metadata) {
		return checkpoint, nil
	}
	return connector.Checkpoint{}, fmt.Errorf("%w: checkpoint has no supported position", connector.ErrCheckpointPosition)
}

func validateCheckpointAdvance(flowID, currentLSN, attemptedLSN string) error {
	if currentLSN == "" {
		return nil
	}
	if attemptedLSN == "" {
		return fmt.Errorf("%w: flow %s cannot replace stream position %s with metadata-only checkpoint", connector.ErrCheckpointRegression, flowID, currentLSN)
	}
	currentCanonical, err := connector.CanonicalizeCheckpointPosition(currentLSN)
	if err != nil {
		return fmt.Errorf("stored checkpoint for flow %s: %w", flowID, err)
	}
	cmp, err := connector.CompareCheckpointLSN(attemptedLSN, currentCanonical)
	if err != nil {
		return err
	}
	if cmp < 0 {
		return fmt.Errorf("%w: flow %s current=%s attempted=%s", connector.ErrCheckpointRegression, flowID, currentCanonical, attemptedLSN)
	}
	return nil
}

func isSupportedControlPosition(metadata map[string]string) bool {
	if metadata == nil {
		return false
	}
	if metadata["mode"] == connector.SourceModeBackfill && strings.TrimSpace(metadata["table"]) != "" {
		return true
	}
	if metadata["control"] != "true" {
		return false
	}
	return strings.TrimSpace(metadata["position"]) != "" || strings.TrimSpace(metadata["seq"]) != ""
}
