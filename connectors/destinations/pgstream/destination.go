package pgstream

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/josephjohncox/wallaby/pkg/connector"
	"github.com/josephjohncox/wallaby/pkg/pgstream"
	"github.com/josephjohncox/wallaby/pkg/schemaregistry"
	"github.com/josephjohncox/wallaby/pkg/wire"
)

const (
	optDSN    = "dsn"
	optStream = "stream"
	optFormat = "format"
)

// Destination writes change events into a Postgres-backed stream.
type Destination struct {
	spec              connector.Spec
	store             *pgstream.Store
	stream            string
	codec             wire.Codec
	registry          schemaregistry.Registry
	registrySubject   string
	protoTypesSubject string
}

func (d *Destination) Open(ctx context.Context, spec connector.Spec) error {
	d.spec = spec
	dsn := spec.Options[optDSN]
	if dsn == "" {
		return errors.New("pgstream dsn is required")
	}
	streamName := spec.Options[optStream]
	if streamName == "" {
		streamName = spec.Name
	}
	if streamName == "" {
		return errors.New("pgstream stream name is required")
	}
	d.stream = streamName

	format := spec.Options[optFormat]
	if format == "" {
		format = string(connector.WireFormatJSON)
	}
	codec, err := wire.NewCodec(format)
	if err != nil {
		return err
	}
	d.codec = codec
	d.registrySubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistrySubject])
	d.protoTypesSubject = strings.TrimSpace(spec.Options[schemaregistry.OptRegistryProtoTypes])
	if d.codec.Name() == connector.WireFormatAvro || d.codec.Name() == connector.WireFormatProto {
		registryCfg := schemaregistry.ConfigFromOptions(spec.Options)
		registry, err := schemaregistry.NewRegistry(ctx, registryCfg)
		if err != nil && !errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			return err
		}
		if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			registry = nil
		}
		d.registry = registry
	}

	store, err := pgstream.NewStore(ctx, dsn)
	if err != nil {
		return err
	}
	d.store = store

	return nil
}

func (d *Destination) Write(ctx context.Context, batch connector.Batch) error {
	if d.store == nil {
		return errors.New("pgstream destination not initialized")
	}
	if len(batch.Records) == 0 {
		return nil
	}
	meta, err := d.ensureSchema(ctx, batch.Schema)
	if err != nil {
		if errors.Is(err, schemaregistry.ErrRegistryDisabled) {
			meta = nil
		} else {
			return err
		}
	}

	messages := make([]pgstream.Message, 0, len(batch.Records))
	for _, record := range batch.Records {
		payloadBatch := connector.Batch{
			Records:    []connector.Record{record},
			Schema:     batch.Schema,
			Checkpoint: batch.Checkpoint,
			WireFormat: batch.WireFormat,
		}
		payload, err := d.codec.Encode(payloadBatch)
		if err != nil {
			return fmt.Errorf("encode stream payload: %w", err)
		}
		if len(payload) == 0 {
			continue
		}

		messages = append(messages, pgstream.Message{
			Stream:          d.stream,
			Namespace:       batch.Schema.Namespace,
			Table:           record.Table,
			LSN:             batch.Checkpoint.LSN,
			WireFormat:      d.codec.Name(),
			Payload:         payload,
			RegistrySubject: registrySubject(meta),
			RegistryID:      registryID(meta),
			RegistryVersion: registryVersion(meta),
		})
	}

	return d.store.Enqueue(ctx, d.stream, messages)
}

func (d *Destination) ApplyDDL(_ context.Context, _ connector.Schema, _ connector.Record) error {
	return nil
}

func (d *Destination) TypeMappings() map[string]string { return nil }

func (d *Destination) Close(_ context.Context) error {
	if d.store != nil {
		d.store.Close()
	}
	if d.registry != nil {
		_ = d.registry.Close()
	}
	return nil
}

func (d *Destination) Capabilities() connector.Capabilities {
	return connector.Capabilities{
		SupportsDDL:           true,
		SupportsSchemaChanges: true,
		SupportsStreaming:     true,
		SupportsBulkLoad:      true,
		SupportsTypeMapping:   true,
		SupportedWireFormats: []connector.WireFormat{
			connector.WireFormatArrow,
			connector.WireFormatParquet,
			connector.WireFormatAvro,
			connector.WireFormatProto,
			connector.WireFormatJSON,
		},
	}
}

type schemaMeta struct {
	Subject string
	ID      string
	Version int
}

func (d *Destination) ensureSchema(ctx context.Context, schema connector.Schema) (*schemaMeta, error) {
	if d.registry == nil {
		return nil, schemaregistry.ErrRegistryDisabled
	}
	subject := d.registrySubjectFor(schema)
	switch d.codec.Name() {
	case connector.WireFormatAvro:
		return d.registerAvroSchema(ctx, subject, schema)
	case connector.WireFormatProto:
		return d.registerProtoSchema(ctx, subject)
	default:
		return nil, schemaregistry.ErrRegistryDisabled
	}
}

func (d *Destination) registerAvroSchema(ctx context.Context, subject string, schema connector.Schema) (*schemaMeta, error) {
	result, err := d.registry.Register(ctx, schemaregistry.RegisterRequest{
		Subject:    subject,
		Schema:     wire.AvroSchema(schema),
		SchemaType: schemaregistry.SchemaTypeAvro,
	})
	if err != nil {
		return nil, err
	}
	return &schemaMeta{Subject: subject, ID: result.ID, Version: result.Version}, nil
}

func (d *Destination) registerProtoSchema(ctx context.Context, subject string) (*schemaMeta, error) {
	def, err := wire.ProtoBatchSchema()
	if err != nil {
		return nil, err
	}
	refNames := make([]string, 0, len(def.Dependencies))
	for name := range def.Dependencies {
		refNames = append(refNames, name)
	}
	sort.Strings(refNames)

	refs := make([]schemaregistry.Reference, 0, len(refNames))
	for _, name := range refNames {
		depSubject := d.protoReferenceSubject(subject, name)
		refResult, err := d.registry.Register(ctx, schemaregistry.RegisterRequest{
			Subject:    depSubject,
			Schema:     def.Dependencies[name],
			SchemaType: schemaregistry.SchemaTypeProtobuf,
		})
		if err != nil {
			return nil, err
		}
		refs = append(refs, schemaregistry.Reference{
			Name:    name,
			Subject: depSubject,
			Version: refResult.Version,
		})
	}

	result, err := d.registry.Register(ctx, schemaregistry.RegisterRequest{
		Subject:    subject,
		Schema:     def.Schema,
		SchemaType: schemaregistry.SchemaTypeProtobuf,
		References: refs,
	})
	if err != nil {
		return nil, err
	}
	return &schemaMeta{Subject: subject, ID: result.ID, Version: result.Version}, nil
}

func (d *Destination) registrySubjectFor(schema connector.Schema) string {
	if d.registrySubject != "" {
		return d.registrySubject
	}
	if schema.Namespace != "" {
		return fmt.Sprintf("%s.%s", schema.Namespace, schema.Name)
	}
	return schema.Name
}

func (d *Destination) protoReferenceSubject(subject, ref string) string {
	if d.protoTypesSubject != "" {
		return d.protoTypesSubject
	}
	name := strings.TrimSuffix(path.Base(ref), ".proto")
	if name == "" {
		name = "types"
	}
	return fmt.Sprintf("%s.%s", subject, name)
}

func registrySubject(meta *schemaMeta) string {
	if meta == nil {
		return ""
	}
	return meta.Subject
}

func registryID(meta *schemaMeta) string {
	if meta == nil {
		return ""
	}
	return meta.ID
}

func registryVersion(meta *schemaMeta) int {
	if meta == nil {
		return 0
	}
	return meta.Version
}
