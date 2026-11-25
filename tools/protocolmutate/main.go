// Command protocolmutate checks that bounded production-protocol tests kill
// specific source mutants. It uses Go overlays and never rewrites the worktree.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"
)

type mutation struct {
	Name     string
	Path     string
	Function string
	Old      string
	New      string
	Package  string
	Tests    string
	Property string
}

type result struct {
	Name       string `json:"name"`
	Property   string `json:"property"`
	Killed     bool   `json:"killed"`
	SourceHash string `json:"source_sha256"`
}

func mutations() []mutation {
	base := mutation{Path: "pkg/stream/runner.go", Function: "func (r *Runner) runManaged(", Package: "./pkg/stream", Tests: "^TestManagedProtocol"}
	makeMutation := func(name, old, replacement, property string) mutation {
		item := base
		item.Name, item.Old, item.New, item.Property = name, old, replacement, property
		return item
	}
	return []mutation{
		makeMutation("retry-definite-errors", "if !errors.Is(err, connector.ErrDeliveryIndeterminate) {", "if false && !errors.Is(err, connector.ErrDeliveryIndeterminate) {", "P4"),
		makeMutation("accept-indeterminate-as-success", "if err == nil {\n\t\t\t\tbreak\n\t\t\t}", "if err == nil || errors.Is(err, connector.ErrDeliveryIndeterminate) {\n\t\t\t\tbreak\n\t\t\t}", "P2/P3"),
		makeMutation("authorize-data-without-target", "if len(transaction.Fragments) == 0 {", "if true {", "P5"),
		makeMutation("deliver-filtered-transaction", "if len(transaction.Fragments) == 0 {", "if false {", "P5"),
		makeMutation("ignore-source-lineage", "if transaction.SourceLineageID == \"\" || transaction.SourceLineageID != expectedLineage {", "if false && (transaction.SourceLineageID == \"\" || transaction.SourceLineageID != expectedLineage) {", "P6"),
		makeMutation("forge-intent-epoch", "LeaseEpoch:            fence.LeaseEpoch,", "LeaseEpoch:            fence.LeaseEpoch + 1,", "P6"),
		makeMutation("feedback-before-durable-delivery", "r.emitCheckpointTrace(ctx, \"read\", transaction.Checkpoint, positionID, readAction, nil)", "r.emitCheckpointTrace(ctx, \"read\", transaction.Checkpoint, positionID, readAction, nil)\n\t\tif err := r.ackManagedGrant(ctx, connector.AckGrant{Checkpoint: transaction.Checkpoint, PositionID: positionID}); err != nil { return err }", "P2/P3"),
		{Name: "accept-unknown-reconciliation-enum", Path: "internal/delivery/coordinator.go", Function: "func (c *Coordinator) reconcileUnfinished(",
			Old: "return connector.DeliveryIndeterminate, connector.DeliveryEvidence{}, fmt.Errorf(\"invalid managed reconciliation disposition %d\", disposition)", New: "return disposition, evidence, nil",
			Package: "./internal/delivery", Tests: "^TestReconciliation", Property: "R-ENUM"},
		{Name: "ignore-coordinator-reconciliation-error", Path: "internal/delivery/coordinator.go", Function: "func (c *Coordinator) reconcileUnfinished(",
			Old: "reconcilepolicy.Decide(uint64(disposition), reconcileErr != nil)", New: "reconcilepolicy.Decide(uint64(disposition), false)",
			Package: "./internal/delivery", Tests: "^TestReconciliation", Property: "R-ERROR"},
		{Name: "ignore-policy-error-flag", Path: "internal/reconcilepolicy/decision.go", Function: "func Decide(",
			Old: "if failed {", New: "if false && failed {",
			Package: "./internal/reconcilepolicy", Tests: "^TestDecide", Property: "D-SEMANTICS"},
	}
}

func patch(source string, item mutation) (string, error) {
	start := strings.Index(source, item.Function)
	if start < 0 {
		return "", fmt.Errorf("function anchor missing for %s", item.Name)
	}
	end := strings.Index(source[start+len(item.Function):], "\nfunc ")
	if end < 0 {
		end = len(source)
	} else {
		end += start + len(item.Function)
	}
	body := source[start:end]
	if count := strings.Count(body, item.Old); count != 1 {
		return "", fmt.Errorf("mutation %s source drift: anchor count=%d, want exactly one", item.Name, count)
	}
	return source[:start] + strings.Replace(body, item.Old, item.New, 1) + source[end:], nil
}

func testCommand(ctx context.Context, overlay, pkg, tests string) ([]byte, error) {
	args := []string{"test", "-mod=readonly", "-count=1", "-timeout=45s"}
	if overlay != "" {
		args = append(args, "-overlay="+overlay)
	}
	args = append(args, pkg, "-run", tests)
	command := exec.CommandContext(ctx, "go", args...)
	return command.CombinedOutput()
}

func killedByProperty(output []byte, testErr error, timedOut bool, property string) bool {
	var exit *exec.ExitError
	return !timedOut && errors.As(testErr, &exit) && strings.Contains(string(output), "--- FAIL:") && strings.Contains(string(output), property) && !strings.Contains(string(output), "[build failed]")
}

func run(outputPath string) error {
	root, err := os.Getwd()
	if err != nil {
		return err
	}
	repository, err := os.OpenRoot(root)
	if err != nil {
		return err
	}
	defer func() { _ = repository.Close() }()
	temporary, err := os.MkdirTemp("", "wallaby-protocol-mutants-")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(temporary) }()
	tempRoot, err := os.OpenRoot(temporary)
	if err != nil {
		return err
	}
	defer func() { _ = tempRoot.Close() }()
	items := mutations()
	baselines := map[string]bool{}
	report := make([]result, 0, len(items))
	for index, item := range items {
		key := item.Package + ":" + item.Tests
		if !baselines[key] {
			ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
			output, err := testCommand(ctx, "", item.Package, item.Tests)
			cancel()
			if err != nil {
				return fmt.Errorf("baseline is not green for %s: %w\n%s", key, err, output)
			}
			baselines[key] = true
		}
		path := filepath.Join(root, item.Path)
		source, err := repository.ReadFile(item.Path)
		if err != nil {
			return err
		}
		updated, err := patch(string(source), item)
		if err != nil {
			return err
		}
		mutantPath := filepath.Join(temporary, fmt.Sprintf("mutant-%d.go", index))
		if err := tempRoot.WriteFile(filepath.Base(mutantPath), []byte(updated), 0o600); err != nil {
			return err
		}
		overlay, err := json.Marshal(map[string]any{"Replace": map[string]string{path: mutantPath}})
		if err != nil {
			return err
		}
		overlayPath := filepath.Join(temporary, "overlay.json")
		if err := tempRoot.WriteFile("overlay.json", overlay, 0o600); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
		output, testErr := testCommand(ctx, overlayPath, item.Package, item.Tests)
		timedOut := ctx.Err() != nil
		cancel()
		if testErr == nil {
			return fmt.Errorf("mutant %s survived property %s", item.Name, item.Property)
		}
		if !killedByProperty(output, testErr, timedOut, item.Property) {
			return fmt.Errorf("mutant %s not killed by property %s (timeout=%v err=%w):\n%s", item.Name, item.Property, timedOut, testErr, output)
		}
		after, err := repository.ReadFile(item.Path)
		if err != nil {
			return err
		}
		if string(after) != string(source) {
			return fmt.Errorf("source changed while checking %s; refusing stale evidence", item.Path)
		}
		report = append(report, result{Name: item.Name, Property: item.Property, Killed: true, SourceHash: fmt.Sprintf("%x", sha256.Sum256(source))})
		fmt.Printf("killed %s with %s\n", item.Name, item.Property)
	}
	if outputPath != "" {
		if err := os.MkdirAll(filepath.Dir(outputPath), 0o700); err != nil {
			return err
		}
		data, err := json.MarshalIndent(report, "", "  ")
		if err != nil {
			return err
		}
		if err := os.WriteFile(outputPath, append(data, '\n'), 0o600); err != nil {
			return err
		}
	}
	fmt.Printf("protocol mutation gate: %d/%d killed; baseline green; no worktree source writes\n", len(report), len(items))
	return nil
}

func main() {
	output := flag.String("output", ".cache/protocol/mutations.json", "machine-readable evidence path")
	flag.Parse()
	if err := run(*output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
