// Package mappingtemplate compiles identifier component templates.
package mappingtemplate

import (
	"errors"
	"fmt"
	"strings"
)

// Component is the one local identifier component a template may reference.
type Component string

const (
	Schema Component = "schema"
	Table  Component = "table"
	Column Component = "column"
)

// Template is an immutable compiled component template.
type Template struct {
	prefix   string
	suffix   string
	compiled bool
}

// Parse compiles raw, requiring exactly one placeholder for component and no
// other braces or placeholders.
func Parse(raw string, component Component) (Template, error) {
	if raw == "" {
		return Template{}, errors.New("template is required")
	}
	if raw != strings.TrimSpace(raw) {
		return Template{}, fmt.Errorf("template %q has leading or trailing whitespace", raw)
	}
	if component != Schema && component != Table && component != Column {
		return Template{}, fmt.Errorf("unsupported template component %q", component)
	}
	placeholder := "{" + string(component) + "}"
	if strings.Count(raw, placeholder) != 1 {
		return Template{}, fmt.Errorf("template %q must contain exactly one %s", raw, placeholder)
	}
	prefix, suffix, _ := strings.Cut(raw, placeholder)
	if strings.ContainsAny(prefix, "{}") || strings.ContainsAny(suffix, "{}") {
		return Template{}, fmt.Errorf("template %q cannot contain placeholders other than %s", raw, placeholder)
	}
	return Template{prefix: prefix, suffix: suffix, compiled: true}, nil
}

// Expand substitutes value without modifying its bytes.
func (t Template) Expand(value string) string {
	if !t.compiled {
		return ""
	}
	return t.prefix + value + t.suffix
}
