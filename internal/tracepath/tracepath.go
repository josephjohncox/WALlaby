// Package tracepath opens per-flow JSONL trace files under an operator-owned
// path template without letting a flow identifier escape the template's root.
package tracepath

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/josephjohncox/wallaby/internal/flow"
)

// Placeholder is replaced by the flow ID in a trace path template.
const Placeholder = "{flow_id}"

// Open creates (truncating) the trace file for flowID.
//
// Every occurrence of Placeholder in template is replaced by flowID; a template
// without the placeholder gets ".<flowID>" appended. The directory portion of
// the template before the first placeholder is the operator-chosen root, taken
// from the template alone. That root must already exist: Open never creates
// it, so a trace path grants no ambient directory-creation authority. The flow
// ID is revalidated here and appears only in the root-relative leaf; all further
// directory and file creation is confined
// to that root with os.Root so separators, dot-dot or symlinks cannot reach
// outside it. Files are created with mode 0600.
func Open(template, flowID string) (*os.File, error) {
	if template == "" {
		return nil, errors.New("trace path template is required")
	}
	if err := flow.ValidateID(flowID); err != nil {
		return nil, fmt.Errorf("trace path: %w", err)
	}

	rootPath, relative := splitTemplate(template, flowID)

	root, err := os.OpenRoot(rootPath)
	if err != nil {
		return nil, fmt.Errorf("open trace root: %w", err)
	}
	defer func() { _ = root.Close() }()
	if dir := filepath.Dir(relative); dir != "." {
		if err := root.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("create trace dir: %w", err)
		}
	}
	file, err := root.OpenFile(relative, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open trace file: %w", err)
	}
	// O_TRUNC preserves an existing file's permissions. Tighten old traces
	// before the caller can write any new CDC data.
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure trace file permissions: %w", err)
	}
	return file, nil
}

// splitTemplate separates the operator-chosen root directory from the
// root-relative trace leaf. rootPath is a slice of template taken before the
// first placeholder and never depends on flowID; flowID is substituted only
// into the leaf, which is later resolved through os.Root.
func splitTemplate(template, flowID string) (rootPath, relative string) {
	prefix := template
	if index := strings.Index(template, Placeholder); index >= 0 {
		prefix = template[:index]
	}
	rootPath = "."
	leafTemplate := template
	if index := strings.LastIndexAny(prefix, `/`+string(os.PathSeparator)); index >= 0 {
		rootPath = template[:index+1]
		leafTemplate = template[index+1:]
	}
	if strings.Contains(leafTemplate, Placeholder) {
		return rootPath, strings.ReplaceAll(leafTemplate, Placeholder, flowID)
	}
	return rootPath, leafTemplate + "." + flowID
}
