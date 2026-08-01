package wire

import (
	"bytes"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/compress"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/josephjohncox/wallaby/pkg/connector"
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

	table := array.NewTableFromRecords(rec.Schema(), []arrow.RecordBatch{rec})
	defer table.Release()

	buf := bytes.NewBuffer(nil)
	props := parquet.NewWriterProperties(
		parquet.WithVersion(parquet.V2_6),
		parquet.WithDataPageVersion(parquet.DataPageV2),
		parquet.WithCompression(compress.Codecs.Zstd),
		parquet.WithCompressionLevel(3),
		parquet.WithDictionaryDefault(true),
		parquet.WithDictionaryPageSizeLimit(1<<20),
		parquet.WithDataPageSize(1<<20),
		parquet.WithBatchSize(1024),
		parquet.WithStats(true),
		parquet.WithCreatedBy("wallaby-canonical-arrow18-zstd3-v2"),
	)
	arrowProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
	if err := pqarrow.WriteTable(table, buf, table.NumRows(), props, arrowProps); err != nil {
		return nil, fmt.Errorf("write parquet: %w", err)
	}

	return buf.Bytes(), nil
}
