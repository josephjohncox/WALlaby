// Package mappingtemplate compiles restricted Go identifier component templates.
package mappingtemplate

import (
	"errors"
	"fmt"
	"strings"
	"text/template"
	"text/template/parse"
)

// Component is the one local identifier component a template may reference.
type Component string

const (
	Schema Component = "schema"
	Table  Component = "table"
	Column Component = "column"
)

// Data is the complete, typed input available while expanding an identifier
// component template. A compiled template may access exactly one field selected
// by its position.
type Data struct {
	Schema string
	Table  string
	Column string
}

// Template is an immutable compiled component template. Parsed templates are
// safe for concurrent execution once construction has completed.
type Template struct {
	compiled *template.Template
	prefix   string
	suffix   string
}

// Parse compiles raw as a restricted text/template. The root may contain text
// nodes and exactly one action whose only argument is the expected Data field.
func Parse(raw string, component Component) (Template, error) {
	if raw == "" {
		return Template{}, errors.New("template is required")
	}
	if raw != strings.TrimSpace(raw) {
		return Template{}, fmt.Errorf("template %q has leading or trailing whitespace", raw)
	}
	if strings.IndexByte(raw, 0) >= 0 {
		return Template{}, fmt.Errorf("template %q cannot contain NUL", raw)
	}

	expected, err := componentField(component)
	if err != nil {
		return Template{}, err
	}
	openCount, closeCount := strings.Count(raw, "{{"), strings.Count(raw, "}}")
	open, close := strings.Index(raw, "{{"), strings.Index(raw, "}}")
	if openCount != 1 || closeCount != 1 || open < 0 || close < open+2 {
		return Template{}, fmt.Errorf("template %q must contain exactly one action delimiter pair", raw)
	}
	if containsTemplateCommentAction(raw) {
		return Template{}, fmt.Errorf("template %q cannot contain template comments", raw)
	}

	compiled, err := template.New("identifier").Parse(raw)
	if err != nil {
		return Template{}, fmt.Errorf("parse template %q: %w", raw, err)
	}
	if len(compiled.Templates()) != 1 || compiled.Tree == nil || compiled.Root == nil {
		return Template{}, fmt.Errorf("template %q may not define or include templates", raw)
	}

	actions := 0
	seenAction := false
	var prefix, suffix strings.Builder
	for _, node := range compiled.Root.Nodes {
		switch node := node.(type) {
		case *parse.TextNode:
			if seenAction {
				suffix.Write(node.Text)
			} else {
				prefix.Write(node.Text)
			}
		case *parse.ActionNode:
			actions++
			seenAction = true
			if err := validateAction(node, expected); err != nil {
				return Template{}, fmt.Errorf("template %q: %w", raw, err)
			}
		default:
			return Template{}, fmt.Errorf("template %q contains unsupported %T", raw, node)
		}
	}
	if actions != 1 {
		return Template{}, fmt.Errorf("template %q must contain exactly one action for .%s", raw, expected)
	}
	return Template{compiled: compiled, prefix: prefix.String(), suffix: suffix.String()}, nil
}

func componentField(component Component) (string, error) {
	switch component {
	case Schema:
		return "Schema", nil
	case Table:
		return "Table", nil
	case Column:
		return "Column", nil
	default:
		return "", fmt.Errorf("unsupported template component %q", component)
	}
}

func validateAction(action *parse.ActionNode, expected string) error {
	pipe := action.Pipe
	if pipe == nil || pipe.IsAssign || len(pipe.Decl) != 0 || len(pipe.Cmds) != 1 {
		return fmt.Errorf("action must be exactly .%s with no declarations or pipeline", expected)
	}
	command := pipe.Cmds[0]
	if command == nil || len(command.Args) != 1 {
		return fmt.Errorf("action must contain exactly one .%s field", expected)
	}
	field, ok := command.Args[0].(*parse.FieldNode)
	if !ok || len(field.Ident) != 1 || field.Ident[0] != expected {
		return fmt.Errorf("action must contain exactly the .%s field", expected)
	}
	return nil
}

// Expand executes the compiled template once with typed data. Injected bytes
// are ordinary field data and are never parsed or recursively interpreted.
func (t Template) Expand(data Data) (string, error) {
	if t.compiled == nil {
		return "", errors.New("template is not compiled")
	}
	var output strings.Builder
	if err := t.compiled.Execute(&output, data); err != nil {
		return "", fmt.Errorf("execute template: %w", err)
	}
	value := output.String()
	if value == "" {
		return "", errors.New("template execution produced an empty identifier")
	}
	if strings.IndexByte(value, 0) >= 0 {
		return "", errors.New("template execution produced an identifier containing NUL")
	}
	return value, nil
}

// Inverse returns the unique component input whose expansion produces output.
// It reports false when output is outside the compiled prefix/suffix range.
func (t Template) Inverse(output string) (string, bool) {
	if t.compiled == nil || !strings.HasPrefix(output, t.prefix) || !strings.HasSuffix(output, t.suffix) {
		return "", false
	}
	end := len(output) - len(t.suffix)
	if end < len(t.prefix) {
		return "", false
	}
	return output[len(t.prefix):end], true
}

func containsTemplateCommentAction(value string) bool {
	for remaining := value; ; {
		open := strings.Index(remaining, "{{")
		if open < 0 {
			return false
		}
		remaining = remaining[open+2:]
		close := strings.Index(remaining, "}}")
		if close < 0 {
			return false
		}
		actionSource := strings.TrimSpace(remaining[:close])
		actionSource = strings.TrimSpace(strings.TrimPrefix(actionSource, "-"))
		actionSource = strings.TrimSpace(strings.TrimSuffix(actionSource, "-"))
		if strings.HasPrefix(actionSource, "/*") {
			return true
		}
		remaining = remaining[close+2:]
	}
}

// ContainsExecutableAction reports whether value is valid Go template text
// whose interpretation would execute template syntax. Invalid template-like
// bytes and ordinary single braces remain literal exact identifier bytes.
func ContainsExecutableAction(value string) bool {
	if containsTemplateCommentAction(value) {
		return true
	}
	compiled, err := template.New("identifier").Parse(value)
	if err != nil || compiled.Tree == nil || compiled.Root == nil {
		return false
	}
	if len(compiled.Templates()) != 1 {
		return true
	}
	for _, node := range compiled.Root.Nodes {
		if _, literal := node.(*parse.TextNode); !literal {
			return true
		}
	}
	return false
}
