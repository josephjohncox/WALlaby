package clickhouse

import (
	"errors"
	"fmt"
	"strings"
)

// maxMetaOrderByBytes bounds meta_order_by before it is split. The eight
// admitted column names plus separators fit well under this limit.
const maxMetaOrderByBytes = 256

// metaTableColumns are the fixed columns of the legacy metadata table, in
// table order. meta_order_by can only select from this set.
var metaTableColumns = []string{"flow_id", "source_schema", "source_table", "synced_at", "is_deleted", "lsn", "operation", "key_json"}

var defaultMetaOrderBy = []string{"flow_id", "source_schema", "source_table", "key_json"}

// metaTableLayout is the compiled engine and ORDER BY of the legacy metadata
// table. It is built only by compileMetaTableLayout, from the fixed MergeTree
// engine and quoted fixed column names; the original option text never reaches
// SQL.
type metaTableLayout struct {
	engine  string
	orderBy string
}

// compileMetaTableLayout admits an empty or case-insensitive "MergeTree"
// engine and a comma-separated list of unique metadata column names, and
// rejects everything else.
func compileMetaTableLayout(engine, orderBy string) (metaTableLayout, error) {
	if engine = strings.TrimSpace(engine); engine != "" && !strings.EqualFold(engine, defaultMetaEngine) {
		return metaTableLayout{}, fmt.Errorf("clickhouse %s must be %s", optMetaEngine, defaultMetaEngine)
	}
	columns := defaultMetaOrderBy
	if orderBy = strings.TrimSpace(orderBy); orderBy != "" {
		if len(orderBy) > maxMetaOrderByBytes {
			return metaTableLayout{}, fmt.Errorf("clickhouse %s exceeds %d bytes", optMetaOrderBy, maxMetaOrderByBytes)
		}
		columns = strings.Split(orderBy, ",")
	}
	allowed := make(map[string]struct{}, len(metaTableColumns))
	for _, column := range metaTableColumns {
		allowed[column] = struct{}{}
	}
	quoted := make([]string, 0, len(columns))
	for _, column := range columns {
		column = strings.TrimSpace(column)
		if _, ok := allowed[column]; !ok {
			return metaTableLayout{}, fmt.Errorf("clickhouse %s must list metadata columns (%s)", optMetaOrderBy, strings.Join(metaTableColumns, ", "))
		}
		delete(allowed, column)
		quoted = append(quoted, quoteIdent(column, '`'))
	}
	return metaTableLayout{engine: defaultMetaEngine, orderBy: strings.Join(quoted, ", ")}, nil
}

func (l metaTableLayout) createTableDDL(tableIdent string) (string, error) {
	if l.engine == "" || l.orderBy == "" {
		return "", errors.New("clickhouse metadata table layout is not compiled")
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
  flow_id String,
  source_schema String,
  source_table String,
  synced_at DateTime64(9),
  is_deleted UInt8,
  lsn String,
  operation String,
  key_json String
) ENGINE = %s ORDER BY (%s)`, tableIdent, l.engine, l.orderBy), nil
}
