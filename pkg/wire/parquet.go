package wire

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/josephjohncox/ductstream/pkg/connector"
)

// ParquetCodec encodes batches as Parquet via Arrow.
type ParquetCodec struct{}

func (c *ParquetCodec) Name() connector.WireFormat {
	return connector.WireFormatParquet
}

func (c *ParquetCodec) ContentType() string {
	return "application/vnd.apache.parquet"
}

func (c *ParquetCodec) Encode(batch connector.Batch) ([]byte, error) {
	rec, err := buildArrowRecord(batch)
	if err != nil {
		return nil, err
	}
	if rec == nil {
		return nil, nil
	}
	defer rec.Release()

	table := array.NewTableFromRecords(rec.Schema(), []arrow.Record{rec})
	defer table.Release()

	buf := bytes.NewBuffer(nil)
	props := parquet.NewWriterProperties()
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	if err := pqarrow.WriteTable(table, buf, table.NumRows(), props, arrowProps); err != nil {
		return nil, fmt.Errorf("write parquet: %w", err)
	}

	return buf.Bytes(), nil
}
