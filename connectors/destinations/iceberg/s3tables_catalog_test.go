package iceberg

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

func TestS3TablesMaintenanceAdmissionAndConfiguration(t *testing.T) {
	t.Parallel()
	t.Run("healthy maintenance", func(t *testing.T) {
		api := healthyS3TablesAPI()
		backend := &s3TablesBackend{maintenance: api, config: testS3TablesConfig()}
		if err := backend.admit(context.Background(), table.Identifier{"lake", "events"}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("retention below reconciliation horizon", func(t *testing.T) {
		api := healthyS3TablesAPI()
		value := api.configuration[string(types.TableMaintenanceTypeIcebergSnapshotManagement)]
		value.Settings = &types.TableMaintenanceSettingsMemberIcebergSnapshotManagement{Value: types.IcebergSnapshotManagementSettings{
			MinSnapshotsToKeep: aws.Int32(1), MaxSnapshotAgeHours: aws.Int32(1),
		}}
		api.configuration[string(types.TableMaintenanceTypeIcebergSnapshotManagement)] = value
		backend := &s3TablesBackend{maintenance: api, config: testS3TablesConfig()}
		if err := backend.admit(context.Background(), table.Identifier{"lake", "events"}); err == nil || !strings.Contains(err.Error(), "min snapshots") {
			t.Fatalf("maintenance error=%v", err)
		}
	})

	t.Run("failed maintenance job", func(t *testing.T) {
		api := healthyS3TablesAPI()
		api.status["icebergCompaction"] = types.TableMaintenanceJobStatusValue{Status: types.JobStatusFailed, FailureMessage: aws.String("broken")}
		backend := &s3TablesBackend{maintenance: api, config: testS3TablesConfig()}
		if err := backend.admit(context.Background(), table.Identifier{"lake", "events"}); err == nil || !strings.Contains(err.Error(), "broken") {
			t.Fatalf("job error=%v", err)
		}
	})

	t.Run("configure through current APIs", func(t *testing.T) {
		api := healthyS3TablesAPI()
		cfg := testS3TablesConfig()
		cfg.S3TablesConfigureMaintenance = true
		backend := &s3TablesBackend{maintenance: api, config: cfg}
		if err := backend.admit(context.Background(), table.Identifier{"lake", "events"}); err != nil {
			t.Fatal(err)
		}
		if len(api.puts) != 2 || api.puts[0] != types.TableMaintenanceTypeIcebergSnapshotManagement || api.puts[1] != types.TableMaintenanceTypeIcebergCompaction {
			t.Fatalf("maintenance puts=%v", api.puts)
		}
	})
}

type fakeS3TablesAPI struct {
	format        types.OpenTableFormat
	configuration map[string]types.TableMaintenanceConfigurationValue
	status        map[string]types.TableMaintenanceJobStatusValue
	puts          []types.TableMaintenanceType
	err           error
}

func healthyS3TablesAPI() *fakeS3TablesAPI {
	return &fakeS3TablesAPI{
		format: types.OpenTableFormatIceberg,
		configuration: map[string]types.TableMaintenanceConfigurationValue{
			string(types.TableMaintenanceTypeIcebergSnapshotManagement): {
				Status: types.MaintenanceStatusEnabled,
				Settings: &types.TableMaintenanceSettingsMemberIcebergSnapshotManagement{Value: types.IcebergSnapshotManagementSettings{
					MinSnapshotsToKeep: aws.Int32(100), MaxSnapshotAgeHours: aws.Int32(24),
				}},
			},
			string(types.TableMaintenanceTypeIcebergCompaction): {
				Status: types.MaintenanceStatusEnabled,
				Settings: &types.TableMaintenanceSettingsMemberIcebergCompaction{Value: types.IcebergCompactionSettings{
					Strategy: types.IcebergCompactionStrategyAuto,
				}},
			},
		},
		status: map[string]types.TableMaintenanceJobStatusValue{
			"icebergSnapshotManagement": {Status: types.JobStatusSuccessful},
			"icebergCompaction":         {Status: types.JobStatusSuccessful},
		},
	}
}

func (api *fakeS3TablesAPI) GetTable(context.Context, *s3tables.GetTableInput, ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error) {
	if api.err != nil {
		return nil, api.err
	}
	return &s3tables.GetTableOutput{Format: api.format}, nil
}

func (api *fakeS3TablesAPI) GetTableMaintenanceConfiguration(context.Context, *s3tables.GetTableMaintenanceConfigurationInput, ...func(*s3tables.Options)) (*s3tables.GetTableMaintenanceConfigurationOutput, error) {
	if api.err != nil {
		return nil, api.err
	}
	return &s3tables.GetTableMaintenanceConfigurationOutput{Configuration: api.configuration}, nil
}

func (api *fakeS3TablesAPI) GetTableMaintenanceJobStatus(context.Context, *s3tables.GetTableMaintenanceJobStatusInput, ...func(*s3tables.Options)) (*s3tables.GetTableMaintenanceJobStatusOutput, error) {
	if api.err != nil {
		return nil, api.err
	}
	return &s3tables.GetTableMaintenanceJobStatusOutput{Status: api.status}, nil
}

func (api *fakeS3TablesAPI) PutTableMaintenanceConfiguration(_ context.Context, input *s3tables.PutTableMaintenanceConfigurationInput, _ ...func(*s3tables.Options)) (*s3tables.PutTableMaintenanceConfigurationOutput, error) {
	if api.err != nil {
		return nil, api.err
	}
	if input == nil || input.Value == nil {
		return nil, errors.New("maintenance value is required")
	}
	api.puts = append(api.puts, input.Type)
	api.configuration[string(input.Type)] = *input.Value
	return &s3tables.PutTableMaintenanceConfigurationOutput{}, nil
}

func testS3TablesConfig() Config {
	cfg := testIcebergConfig()
	cfg.Profile = CatalogProfileS3Tables
	cfg.Region = "us-east-1"
	cfg.Warehouse = "123456789012:s3tablescatalog/wallaby"
	cfg.S3TablesTableBucketARN = "arn:aws:s3tables:us-east-1:123456789012:bucket/wallaby"
	cfg.S3TablesMinSnapshotsToKeep = 100
	cfg.S3TablesMaxSnapshotAgeHours = 24
	return cfg
}
