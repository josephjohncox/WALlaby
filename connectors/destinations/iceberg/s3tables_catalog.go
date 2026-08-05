package iceberg

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	iceberggo "github.com/apache/iceberg-go"
	"github.com/apache/iceberg-go/table"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3tables"
	"github.com/aws/aws-sdk-go-v2/service/s3tables/types"
)

type s3TablesAPI interface {
	GetTable(context.Context, *s3tables.GetTableInput, ...func(*s3tables.Options)) (*s3tables.GetTableOutput, error)
	GetTableMaintenanceConfiguration(context.Context, *s3tables.GetTableMaintenanceConfigurationInput, ...func(*s3tables.Options)) (*s3tables.GetTableMaintenanceConfigurationOutput, error)
	GetTableMaintenanceJobStatus(context.Context, *s3tables.GetTableMaintenanceJobStatusInput, ...func(*s3tables.Options)) (*s3tables.GetTableMaintenanceJobStatusOutput, error)
	PutTableMaintenanceConfiguration(context.Context, *s3tables.PutTableMaintenanceConfigurationInput, ...func(*s3tables.Options)) (*s3tables.PutTableMaintenanceConfigurationOutput, error)
}

type s3TablesBackend struct {
	rest        *restBackend
	maintenance s3TablesAPI
	config      Config
}

// NewS3TablesCommitter uses the AWS Glue Iceberg REST endpoint for catalog
// commits and the current S3 Tables control APIs for maintenance admission.
// Managed-table files never become Wallaby artifact roots.
func NewS3TablesCommitter(ctx context.Context, objects CanonicalObjectReader, config Config) (*Committer, error) {
	if config.Profile != CatalogProfileS3Tables {
		return nil, errors.New("S3 Tables committer requires catalog_profile=s3tables")
	}
	rest, err := newRESTBackend(ctx, config)
	if err != nil {
		return nil, err
	}
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(config.Region))
	if err != nil {
		return nil, fmt.Errorf("load S3 Tables AWS config: %w", err)
	}
	backend := &s3TablesBackend{
		rest: rest, maintenance: s3tables.NewFromConfig(awsCfg), config: config,
	}
	return NewCommitter(objects, backend, config)
}

func (backend *s3TablesBackend) Load(ctx context.Context, identifier table.Identifier) (catalogTable, error) {
	state, err := backend.rest.Load(ctx, identifier)
	if err != nil {
		return catalogTable{}, err
	}
	if err := backend.admit(ctx, identifier); err != nil {
		return catalogTable{}, err
	}
	return state, nil
}

func (backend *s3TablesBackend) Create(ctx context.Context, identifier table.Identifier, schema *iceberggo.Schema) (catalogTable, error) {
	state, err := backend.rest.Create(ctx, identifier, schema)
	if err != nil {
		return catalogTable{}, err
	}
	if err := backend.admit(ctx, identifier); err != nil {
		return catalogTable{}, err
	}
	return state, nil
}

func (backend *s3TablesBackend) Evolve(ctx context.Context, state catalogTable, adds []iceberggo.NestedField, renames []renameOp) (catalogTable, error) {
	if err := backend.admit(ctx, state.Identifier); err != nil {
		return catalogTable{}, err
	}
	return backend.rest.Evolve(ctx, state, adds, renames)
}

func (backend *s3TablesBackend) Append(ctx context.Context, state catalogTable, schema *iceberggo.Schema, records []arrow.RecordBatch, summary map[string]string) (catalogSnapshot, error) {
	if err := backend.admit(ctx, state.Identifier); err != nil {
		return catalogSnapshot{}, err
	}
	return backend.rest.Append(ctx, state, schema, records, summary)
}

func (backend *s3TablesBackend) admit(ctx context.Context, identifier table.Identifier) error {
	if len(identifier) != 2 {
		return fmt.Errorf("S3 Tables requires exactly one namespace component and table name; got %q", strings.Join(identifier, "."))
	}
	namespace, name := identifier[0], identifier[1]
	tableOutput, err := backend.maintenance.GetTable(ctx, &s3tables.GetTableInput{
		TableBucketARN: aws.String(backend.config.S3TablesTableBucketARN),
		Namespace:      aws.String(namespace), Name: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("inspect S3 Tables table %s: %w", strings.Join(identifier, "."), err)
	}
	if tableOutput.Format != types.OpenTableFormatIceberg {
		return fmt.Errorf("S3 Tables table %s format=%q, require ICEBERG", strings.Join(identifier, "."), tableOutput.Format)
	}
	if backend.config.S3TablesConfigureMaintenance {
		if err := backend.configureMaintenance(ctx, namespace, name); err != nil {
			return err
		}
	}
	configuration, err := backend.maintenance.GetTableMaintenanceConfiguration(ctx, &s3tables.GetTableMaintenanceConfigurationInput{
		TableBucketARN: aws.String(backend.config.S3TablesTableBucketARN),
		Namespace:      aws.String(namespace), Name: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("inspect S3 Tables maintenance for %s: %w", strings.Join(identifier, "."), err)
	}
	if err := backend.validateMaintenance(configuration.Configuration); err != nil {
		return fmt.Errorf("S3 Tables table %s: %w", strings.Join(identifier, "."), err)
	}
	status, err := backend.maintenance.GetTableMaintenanceJobStatus(ctx, &s3tables.GetTableMaintenanceJobStatusInput{
		TableBucketARN: aws.String(backend.config.S3TablesTableBucketARN),
		Namespace:      aws.String(namespace), Name: aws.String(name),
	})
	if err != nil {
		return fmt.Errorf("inspect S3 Tables maintenance status for %s: %w", strings.Join(identifier, "."), err)
	}
	for maintenanceType, job := range status.Status {
		if job.Status == types.JobStatusFailed {
			return fmt.Errorf("S3 Tables maintenance %s failed: %s", maintenanceType, aws.ToString(job.FailureMessage))
		}
	}
	return nil
}

func (backend *s3TablesBackend) configureMaintenance(ctx context.Context, namespace, name string) error {
	common := func(maintenanceType types.TableMaintenanceType, value *types.TableMaintenanceConfigurationValue) error {
		_, err := backend.maintenance.PutTableMaintenanceConfiguration(ctx, &s3tables.PutTableMaintenanceConfigurationInput{
			TableBucketARN: aws.String(backend.config.S3TablesTableBucketARN),
			Namespace:      aws.String(namespace), Name: aws.String(name), Type: maintenanceType, Value: value,
		})
		return err
	}
	snapshot := &types.TableMaintenanceConfigurationValue{
		Status: types.MaintenanceStatusEnabled,
		Settings: &types.TableMaintenanceSettingsMemberIcebergSnapshotManagement{Value: types.IcebergSnapshotManagementSettings{
			MinSnapshotsToKeep:  aws.Int32(backend.config.S3TablesMinSnapshotsToKeep),
			MaxSnapshotAgeHours: aws.Int32(backend.config.S3TablesMaxSnapshotAgeHours),
		}},
	}
	if err := common(types.TableMaintenanceTypeIcebergSnapshotManagement, snapshot); err != nil {
		return fmt.Errorf("configure S3 Tables snapshot retention: %w", err)
	}
	compaction := &types.TableMaintenanceConfigurationValue{
		Status: types.MaintenanceStatusEnabled,
		Settings: &types.TableMaintenanceSettingsMemberIcebergCompaction{Value: types.IcebergCompactionSettings{
			Strategy: types.IcebergCompactionStrategyAuto,
		}},
	}
	if err := common(types.TableMaintenanceTypeIcebergCompaction, compaction); err != nil {
		return fmt.Errorf("configure S3 Tables compaction: %w", err)
	}
	return nil
}

func (backend *s3TablesBackend) validateMaintenance(configuration map[string]types.TableMaintenanceConfigurationValue) error {
	snapshot, ok := configuration[string(types.TableMaintenanceTypeIcebergSnapshotManagement)]
	if !ok || snapshot.Status != types.MaintenanceStatusEnabled {
		return errors.New("snapshot management must be enabled")
	}
	snapshotSettings, ok := snapshot.Settings.(*types.TableMaintenanceSettingsMemberIcebergSnapshotManagement)
	if !ok {
		return errors.New("snapshot management settings are missing")
	}
	if aws.ToInt32(snapshotSettings.Value.MinSnapshotsToKeep) < backend.config.S3TablesMinSnapshotsToKeep {
		return fmt.Errorf("min snapshots to keep=%d, require at least %d", aws.ToInt32(snapshotSettings.Value.MinSnapshotsToKeep), backend.config.S3TablesMinSnapshotsToKeep)
	}
	if aws.ToInt32(snapshotSettings.Value.MaxSnapshotAgeHours) < backend.config.S3TablesMaxSnapshotAgeHours {
		return fmt.Errorf("max snapshot age=%dh, require at least %dh", aws.ToInt32(snapshotSettings.Value.MaxSnapshotAgeHours), backend.config.S3TablesMaxSnapshotAgeHours)
	}
	compaction, ok := configuration[string(types.TableMaintenanceTypeIcebergCompaction)]
	if !ok || compaction.Status != types.MaintenanceStatusEnabled {
		return errors.New("iceberg compaction must be enabled")
	}
	if _, ok := compaction.Settings.(*types.TableMaintenanceSettingsMemberIcebergCompaction); !ok {
		return errors.New("iceberg compaction settings are missing")
	}
	return nil
}
