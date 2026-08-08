package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"github.com/josephjohncox/wallaby/internal/endpointcodec"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
	"gopkg.in/yaml.v3"
)

// endpointConfig is the authoring representation of the public Endpoint
// protobuf. Its custom decoders make JSON and YAML use the same strict,
// canonical snake_case field vocabulary.
type endpointConfig struct {
	endpoint *wallabypb.Endpoint
}

func (c *endpointConfig) UnmarshalJSON(data []byte) error {
	if c == nil {
		return errors.New("endpoint target is nil")
	}
	endpoint := &wallabypb.Endpoint{}
	if err := validateCanonicalProtoJSON(data, endpoint.ProtoReflect().Descriptor(), "endpoint"); err != nil {
		return err
	}
	if err := (protojson.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(data, endpoint); err != nil {
		return err
	}
	c.endpoint = endpoint
	return nil
}

func (c endpointConfig) MarshalJSON() ([]byte, error) {
	if c.endpoint == nil {
		return []byte("null"), nil
	}
	return (protojson.MarshalOptions{UseProtoNames: true}).Marshal(c.endpoint)
}

func (c *endpointConfig) UnmarshalYAML(node *yaml.Node) error {
	if err := validateYAMLNode(node); err != nil {
		return err
	}
	var value any
	if err := node.Decode(&value); err != nil {
		return err
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("convert endpoint YAML to JSON: %w", err)
	}
	return c.UnmarshalJSON(encoded)
}

func (c endpointConfig) MarshalYAML() (any, error) {
	encoded, err := c.MarshalJSON()
	if err != nil {
		return nil, err
	}
	var value any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func (c endpointConfig) toProto(role endpointcodec.Role) (*wallabypb.Endpoint, error) {
	if c.endpoint == nil {
		return nil, errors.New("endpoint is required")
	}
	endpoint := endpointcodec.Clone(c.endpoint)
	if _, err := endpointcodec.Decode(endpoint, role); err != nil {
		return nil, err
	}
	return endpoint, nil
}

// redactEndpointProto walks the typed endpoint descriptor recursively. Secret
// classification follows protobuf field identity rather than the runtime
// adapter's free-form option keys. Custom options are intentionally all secret.
func redactEndpointProto(endpoint *wallabypb.Endpoint) *wallabypb.Endpoint {
	redacted := endpointcodec.Clone(endpoint)
	if redacted == nil {
		return nil
	}
	redactEndpointMessage(redacted.ProtoReflect())
	return redacted
}

func redactEndpointMessage(message protoreflect.Message) {
	fields := message.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if !message.Has(field) {
			continue
		}
		if field.IsMap() {
			redactEndpointMap(message.Mutable(field).Map(), field)
			continue
		}
		if field.IsList() {
			list := message.Mutable(field).List()
			if field.Kind() == protoreflect.MessageKind {
				for item := 0; item < list.Len(); item++ {
					redactEndpointMessage(list.Get(item).Message())
				}
			}
			continue
		}
		if field.Kind() == protoreflect.MessageKind {
			redactEndpointMessage(message.Mutable(field).Message())
			continue
		}
		if field.Kind() != protoreflect.StringKind {
			continue
		}
		value := message.Get(field).String()
		switch classifyEndpointProtoField(field) {
		case endpointOptionSensitive:
			message.Set(field, protoreflect.ValueOfString(redactedEndpointOption))
		case endpointOptionURL:
			message.Set(field, protoreflect.ValueOfString(sanitizeEndpointURL(value)))
		case endpointOptionNetwork:
			message.Set(field, protoreflect.ValueOfString(sanitizeEndpointNetworkValue(value)))
		default:
			if endpointURLHasSecrets(value) {
				message.Set(field, protoreflect.ValueOfString(redactedEndpointOption))
			}
		}
	}
}

func redactEndpointMap(mapping protoreflect.Map, field protoreflect.FieldDescriptor) {
	redactAll := sensitiveEndpointProtoField(field) ||
		(field.ContainingMessage().FullName() == "wallaby.v1.CustomEndpointConfig" && field.Name() == "options")
	if field.MapValue().Kind() == protoreflect.MessageKind {
		mapping.Range(func(_ protoreflect.MapKey, value protoreflect.Value) bool {
			redactEndpointMessage(value.Message())
			return true
		})
		return
	}
	if !redactAll || field.MapValue().Kind() != protoreflect.StringKind {
		return
	}
	keys := make([]protoreflect.MapKey, 0, mapping.Len())
	mapping.Range(func(key protoreflect.MapKey, _ protoreflect.Value) bool {
		keys = append(keys, key)
		return true
	})
	for _, key := range keys {
		mapping.Set(key, protoreflect.ValueOfString(redactedEndpointOption))
	}
}

func classifyEndpointProtoField(field protoreflect.FieldDescriptor) endpointOptionValueClass {
	if sensitiveEndpointProtoField(field) {
		return endpointOptionSensitive
	}
	return classifyEndpointOptionKey(string(field.Name()))
}

func sensitiveEndpointProtoField(field protoreflect.FieldDescriptor) bool {
	name := normalizedEndpointOptionKey(string(field.Name()))
	if field.ContainingMessage().FullName() == "wallaby.v1.CustomEndpointConfig" && field.Name() == "options" {
		return true
	}
	if name == "header" || name == "headers" || name == "metadata" || name == "externalid" || name == "roleexternalid" {
		return true
	}
	for _, marker := range []string{"dsn", "password", "passwd", "secret", "credential", "privatekey", "accesskey", "apikey", "authorization", "bearer", "connectionstring", "clientkey", "sslkey", "signingkey", "encryptionkey"} {
		if strings.Contains(name, marker) {
			return true
		}
	}
	if strings.Contains(name, "token") && !strings.Contains(name, "endpoint") && !strings.Contains(name, "url") && !strings.Contains(name, "uri") {
		return true
	}
	return hasEndpointOptionSuffix(name, "keyfile", "passwordfile", "tokenfile", "secretfile", "credentialfile", "credentialsfile")
}

func validateYAMLNode(node *yaml.Node) error {
	if node == nil {
		return errors.New("empty YAML endpoint")
	}
	if node.Kind == yaml.AliasNode || node.Alias != nil {
		return errors.New("YAML aliases are not allowed")
	}
	if node.Kind == yaml.MappingNode {
		seen := make(map[string]struct{}, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				return errors.New("YAML mapping keys must be strings")
			}
			if _, duplicate := seen[key.Value]; duplicate {
				return fmt.Errorf("duplicate YAML key %q", key.Value)
			}
			seen[key.Value] = struct{}{}
		}
	}
	for _, child := range node.Content {
		if err := validateYAMLNode(child); err != nil {
			return err
		}
	}
	return nil
}

func validateCanonicalProtoJSON(data []byte, descriptor protoreflect.MessageDescriptor, path string) error {
	if err := validateJSONDocument(data); err != nil {
		return err
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err == nil {
		return errors.New("multiple JSON values are not allowed")
	}
	return validateCanonicalProtoValue(value, descriptor, path)
}

func validateCanonicalProtoValue(value any, descriptor protoreflect.MessageDescriptor, path string) error {
	object, ok := value.(map[string]any)
	if !ok {
		return fmt.Errorf("%s must be an object", path)
	}
	fields := descriptor.Fields()
	byName := make(map[string]protoreflect.FieldDescriptor, fields.Len())
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		byName[string(field.Name())] = field
	}
	for name, child := range object {
		field, exists := byName[name]
		if !exists {
			return fmt.Errorf("%s.%s is unknown or is not canonical snake_case", path, name)
		}
		childPath := path + "." + name
		if child == nil {
			return fmt.Errorf("%s must not be null", childPath)
		}
		if field.IsMap() {
			mapping, ok := child.(map[string]any)
			if !ok {
				return fmt.Errorf("%s must be an object", childPath)
			}
			for key, item := range mapping {
				if field.MapValue().Kind() == protoreflect.StringKind {
					if _, ok := item.(string); !ok {
						return fmt.Errorf("%s[%q] must be a string", childPath, key)
					}
				} else if field.MapValue().Kind() == protoreflect.MessageKind {
					if err := validateCanonicalProtoValue(item, field.MapValue().Message(), childPath+"["+key+"]"); err != nil {
						return err
					}
				}
			}
			continue
		}
		if field.IsList() {
			items, ok := child.([]any)
			if !ok {
				return fmt.Errorf("%s must be an array", childPath)
			}
			for index, item := range items {
				if err := validateCanonicalFieldValue(item, field, fmt.Sprintf("%s[%d]", childPath, index)); err != nil {
					return err
				}
			}
			continue
		}
		if err := validateCanonicalFieldValue(child, field, childPath); err != nil {
			return err
		}
	}
	return nil
}

func validateCanonicalFieldValue(value any, field protoreflect.FieldDescriptor, path string) error {
	switch field.Kind() {
	case protoreflect.MessageKind:
		if field.Message().FullName() == "google.protobuf.Duration" {
			if _, ok := value.(string); !ok {
				return fmt.Errorf("%s must be a duration string", path)
			}
			return nil
		}
		return validateCanonicalProtoValue(value, field.Message(), path)
	case protoreflect.EnumKind:
		name, ok := value.(string)
		if !ok {
			return fmt.Errorf("%s must use a canonical enum name", path)
		}
		if field.Enum().Values().ByName(protoreflect.Name(name)) == nil {
			return fmt.Errorf("%s has unknown enum value %q", path, name)
		}
	case protoreflect.StringKind:
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a string", path)
		}
	case protoreflect.BoolKind:
		if _, ok := value.(bool); !ok {
			return fmt.Errorf("%s must be a boolean", path)
		}
	case protoreflect.DoubleKind, protoreflect.FloatKind,
		protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
		protoreflect.Uint32Kind, protoreflect.Fixed32Kind:
		if _, ok := value.(json.Number); !ok {
			return fmt.Errorf("%s must be a number", path)
		}
	case protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind,
		protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
		// Proto JSON represents 64-bit integers as decimal strings.
		if _, ok := value.(string); !ok {
			return fmt.Errorf("%s must be a decimal string", path)
		}
	}
	return nil
}
