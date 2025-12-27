package schema

// ChangeType describes the type of schema evolution event.
type ChangeType string

const (
	ChangeAddColumn        ChangeType = "add_column"
	ChangeDropColumn       ChangeType = "drop_column"
	ChangeAlterColumn      ChangeType = "alter_column"
	ChangeRenameColumn     ChangeType = "rename_column"
	ChangeSetGenerated     ChangeType = "set_generated"
	ChangeDropGenerated    ChangeType = "drop_generated"
	ChangeCreateTable      ChangeType = "create_table"
	ChangeDropTable        ChangeType = "drop_table"
	ChangeCreateIndex      ChangeType = "create_index"
	ChangeDropIndex        ChangeType = "drop_index"
	ChangeAlterPrimaryKey  ChangeType = "alter_primary_key"
	ChangeAlterForeignKey  ChangeType = "alter_foreign_key"
	ChangeAlterConstraints ChangeType = "alter_constraints"
)

// Change captures a schema change to be applied downstream.
type Change struct {
	Type        ChangeType
	Namespace   string
	Table       string
	Column      string
	FromType    string
	ToType      string
	Expression  string
	Nullable    bool
	PrimaryKeys []string
}

// Plan groups multiple schema changes.
type Plan struct {
	Changes []Change
}
