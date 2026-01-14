package schemaregistry

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/glue"
	gluetypes "github.com/aws/aws-sdk-go-v2/service/glue/types"
	"github.com/aws/aws-sdk-go-v2/service/sts"
	"github.com/aws/smithy-go"
)

type glueRegistry struct {
	client   *glue.Client
	registry string
	schema   string
}

func newGlueRegistry(ctx context.Context, cfg Config) (*glueRegistry, error) {
	loadOpts := []func(*config.LoadOptions) error{}
	if cfg.Region != "" {
		loadOpts = append(loadOpts, config.WithRegion(cfg.Region))
	}
	if cfg.Profile != "" {
		loadOpts = append(loadOpts, config.WithSharedConfigProfile(cfg.Profile))
	}
	awsCfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, fmt.Errorf("load aws config: %w", err)
	}
	if cfg.RoleARN != "" {
		stsClient := sts.NewFromConfig(awsCfg)
		awsCfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(stsClient, cfg.RoleARN))
	}
	client := glue.NewFromConfig(awsCfg, func(o *glue.Options) {
		if cfg.Endpoint != "" {
			o.BaseEndpoint = aws.String(cfg.Endpoint)
		}
	})
	registry := cfg.GlueRegistry
	if registry == "" {
		registry = "wallaby"
	}
	return &glueRegistry{
		client:   client,
		registry: registry,
		schema:   cfg.GlueSchema,
	}, nil
}

func (r *glueRegistry) Register(ctx context.Context, req RegisterRequest) (RegisterResult, error) {
	if req.Subject == "" {
		return RegisterResult{}, fmt.Errorf("schema registry subject is required")
	}
	if req.Schema == "" {
		return RegisterResult{}, fmt.Errorf("schema registry schema is required")
	}
	schemaName := r.schema
	if schemaName == "" {
		schemaName = sanitizeGlueSchemaName(req.Subject)
	}
	schemaID := &gluetypes.SchemaId{
		RegistryName: aws.String(r.registry),
		SchemaName:   aws.String(schemaName),
	}
	dataFormat := glueDataFormat(req.SchemaType)
	if err := r.ensureSchema(ctx, schemaID, dataFormat, req.Schema); err != nil {
		return RegisterResult{}, err
	}
	resp, err := r.client.RegisterSchemaVersion(ctx, &glue.RegisterSchemaVersionInput{
		SchemaId:         schemaID,
		SchemaDefinition: aws.String(req.Schema),
	})
	if err != nil {
		return RegisterResult{}, fmt.Errorf("register glue schema version: %w", err)
	}
	version := 0
	if resp.VersionNumber != nil {
		version = int(*resp.VersionNumber)
	}
	id := ""
	if resp.SchemaVersionId != nil {
		id = *resp.SchemaVersionId
	}
	return RegisterResult{ID: id, Version: version}, nil
}

func (r *glueRegistry) ensureSchema(ctx context.Context, schemaID *gluetypes.SchemaId, format gluetypes.DataFormat, schema string) error {
	registryID := &gluetypes.RegistryId{RegistryName: schemaID.RegistryName}
	_, err := r.client.CreateSchema(ctx, &glue.CreateSchemaInput{
		RegistryId:       registryID,
		SchemaName:       schemaID.SchemaName,
		DataFormat:       format,
		Compatibility:    gluetypes.CompatibilityNone,
		SchemaDefinition: aws.String(schema),
	})
	if err == nil {
		return nil
	}
	if isAlreadyExists(err) {
		return nil
	}
	return fmt.Errorf("create glue schema: %w", err)
}

func (r *glueRegistry) Close() error { return nil }

func glueDataFormat(schemaType SchemaType) gluetypes.DataFormat {
	switch schemaType {
	case SchemaTypeProtobuf:
		return gluetypes.DataFormatProtobuf
	default:
		return gluetypes.DataFormatAvro
	}
}

func isAlreadyExists(err error) bool {
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		return strings.EqualFold(apiErr.ErrorCode(), "AlreadyExistsException")
	}
	return false
}

func sanitizeGlueSchemaName(subject string) string {
	sanitized := strings.TrimSpace(subject)
	sanitized = strings.ReplaceAll(sanitized, " ", "_")
	sanitized = strings.ReplaceAll(sanitized, ".", "_")
	sanitized = strings.ReplaceAll(sanitized, "/", "_")
	sanitized = strings.ReplaceAll(sanitized, "-", "_")
	if sanitized == "" {
		return "wallaby_schema"
	}
	return sanitized
}
