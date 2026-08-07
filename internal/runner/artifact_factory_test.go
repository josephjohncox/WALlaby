package runner

import (
	"context"
	"math"
	"strconv"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/josephjohncox/wallaby/internal/config"
	"github.com/josephjohncox/wallaby/internal/flow"
	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/stream"
)

func TestArtifactFactoryRejectsIcebergBoundsBeforeObjectStoreIO(t *testing.T) {
	t.Parallel()
	if strconv.IntSize < 64 {
		t.Skip("int32 overflow input requires a 64-bit test process")
	}
	factory := NewArtifactLogFactory(&pgxpool.Pool{}, config.ArtifactConfig{Bucket: "must-not-be-contacted"}, config.IcebergConfig{
		S3TablesMinSnapshotsToKeep: int(int64(math.MaxInt32) + 1),
	})
	_, err := factory(context.Background(), flow.Flow{}, []stream.DestinationConfig{{
		Spec: connector.RuntimeSpec{Name: "iceberg", Type: connector.EndpointIceberg},
	}})
	if err == nil || !strings.Contains(err.Error(), "s3_tables_min_snapshots_to_keep") {
		t.Fatalf("artifact factory error=%v, want pre-I/O Iceberg bounds rejection", err)
	}
}

func TestIcebergDestinationConfigChecksInt32Bounds(t *testing.T) {
	t.Parallel()

	valid, err := icebergDestinationConfig(config.IcebergConfig{
		S3TablesMinSnapshotsToKeep:  1,
		S3TablesMaxSnapshotAgeHours: math.MaxInt32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if valid.S3TablesMinSnapshotsToKeep != 1 || valid.S3TablesMaxSnapshotAgeHours != math.MaxInt32 {
		t.Fatalf("converted maintenance bounds=%d/%d", valid.S3TablesMinSnapshotsToKeep, valid.S3TablesMaxSnapshotAgeHours)
	}

	if strconv.IntSize < 64 {
		return
	}
	aboveMax := int64(math.MaxInt32) + 1
	belowMin := int64(math.MinInt32) - 1
	for _, test := range []struct {
		name string
		cfg  config.IcebergConfig
	}{
		{name: "minimum snapshots overflow", cfg: config.IcebergConfig{S3TablesMinSnapshotsToKeep: int(aboveMax)}},
		{name: "maximum age overflow", cfg: config.IcebergConfig{S3TablesMaxSnapshotAgeHours: int(aboveMax)}},
		{name: "minimum snapshots underflow", cfg: config.IcebergConfig{S3TablesMinSnapshotsToKeep: int(belowMin)}},
		{name: "maximum age underflow", cfg: config.IcebergConfig{S3TablesMaxSnapshotAgeHours: int(belowMin)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := icebergDestinationConfig(test.cfg); err == nil {
				t.Fatal("out-of-range Iceberg maintenance value was accepted")
			}
		})
	}
}
