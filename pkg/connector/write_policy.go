package connector

// ResolvedWriteMode is the destination write behavior for one projected table.
type ResolvedWriteMode string

const (
	ResolvedWriteAppend ResolvedWriteMode = "append"
	ResolvedWriteUpsert ResolvedWriteMode = "upsert"
)

const (
	AppendOperationColumn      = "__wallaby_operation"
	AppendDeletedColumn        = "__wallaby_deleted"
	AppendSourcePositionColumn = "__wallaby_source_position"
)

// TableWritePolicy is the fully resolved write contract carried with a projected batch.
type TableWritePolicy struct {
	Mode                  ResolvedWriteMode
	KeyColumns            []string
	WatermarkColumn       string
	ProjectionFingerprint string
}

// IsZero reports whether a batch has no resolved projection policy.
func (p TableWritePolicy) IsZero() bool {
	return p.Mode == "" && len(p.KeyColumns) == 0 && p.WatermarkColumn == "" && p.ProjectionFingerprint == ""
}
