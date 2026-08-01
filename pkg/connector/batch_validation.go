package connector

import (
	"errors"
	"fmt"
	"strings"
)

// ErrInvalidBatch identifies a connector batch that cannot be interpreted
// against one table-level schema without reordering or guessing.
var ErrInvalidBatch = errors.New("invalid connector batch")

// ValidateBatch enforces the source-to-runner batch contract. Data batches
// describe exactly one table and one logical schema. DDL/control records may be
// grouped together, but never with data records. Tableless control batches are
// valid because PostgreSQL logical messages carry ordered DDL text and a source
// position without relation metadata. A zero record schema version is treated as
// inherited from Batch.Schema for adapters that omit the redundant field.
func ValidateBatch(batch Batch) error {
	if len(batch.Records) == 0 {
		return nil
	}

	control := isBatchControlRecord(batch.Records[0])
	for index, record := range batch.Records {
		if isBatchControlRecord(record) != control {
			return fmt.Errorf("%w: batch mixes data and control records at record %d", ErrInvalidBatch, index)
		}
	}

	schemaName := strings.TrimSpace(batch.Schema.Name)
	if control && schemaName == "" {
		for index, record := range batch.Records {
			if strings.TrimSpace(record.Table) != "" || record.SchemaVersion != 0 {
				return fmt.Errorf("%w: table-scoped control record %d requires a batch schema", ErrInvalidBatch, index)
			}
		}
		return nil
	}
	if schemaName == "" {
		return fmt.Errorf("%w: schema name is required for a non-empty data batch", ErrInvalidBatch)
	}

	for index, record := range batch.Records {
		if strings.TrimSpace(record.Table) == "" {
			return fmt.Errorf("%w: record %d table is required", ErrInvalidBatch, index)
		}
		if record.Table != batch.Schema.Name {
			return fmt.Errorf(
				"%w: record %d table %q does not match batch schema table %q",
				ErrInvalidBatch,
				index,
				record.Table,
				batch.Schema.Name,
			)
		}
		if record.SchemaVersion != 0 && record.SchemaVersion != batch.Schema.Version {
			return fmt.Errorf(
				"%w: record %d schema version %d does not match batch schema version %d",
				ErrInvalidBatch,
				index,
				record.SchemaVersion,
				batch.Schema.Version,
			)
		}
	}

	return nil
}

func isBatchControlRecord(record Record) bool {
	return record.Operation == OpDDL || record.DDL != "" || len(record.DDLPlan) > 0
}
