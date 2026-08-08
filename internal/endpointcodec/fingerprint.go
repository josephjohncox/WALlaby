package endpointcodec

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	wallabypb "github.com/josephjohncox/wallaby/gen/go/wallaby/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const destinationFingerprintDomain = "wallaby.destination-config.v2.typed"

// DeliveryConfigFingerprint fingerprints only the persisted typed destination
// branch and immutable logical projection. Runtime enrichment cannot enter the
// domain because the domain owns the protobuf endpoint directly.
func DeliveryConfigFingerprint(endpoint *wallabypb.Endpoint, projectionFingerprint string) (string, error) {
	if strings.TrimSpace(projectionFingerprint) == "" {
		return "", errors.New("projection fingerprint is required")
	}
	if _, err := Decode(endpoint, RoleDestination); err != nil {
		return "", fmt.Errorf("validate typed destination fingerprint: %w", err)
	}
	endpoint = Clone(endpoint)
	clearFieldByName(endpoint.ProtoReflect(), "destination_revision_id")
	config, err := (protojson.MarshalOptions{UseProtoNames: true}).Marshal(endpoint)
	if err != nil {
		return "", fmt.Errorf("marshal typed destination fingerprint: %w", err)
	}
	payload, err := json.Marshal(struct {
		Domain     string          `json:"domain"`
		Endpoint   json.RawMessage `json:"endpoint"`
		Projection string          `json:"projection_fingerprint"`
	}{Domain: destinationFingerprintDomain, Endpoint: config, Projection: projectionFingerprint})
	if err != nil {
		return "", fmt.Errorf("encode delivery config fingerprint: %w", err)
	}
	digest := sha256.Sum256(payload)
	return hex.EncodeToString(digest[:]), nil
}

func clearFieldByName(message protoreflect.Message, name protoreflect.Name) {
	fields := message.Descriptor().Fields()
	for index := 0; index < fields.Len(); index++ {
		field := fields.Get(index)
		if field.Name() == name {
			message.Clear(field)
			continue
		}
		if field.IsMap() || !message.Has(field) {
			continue
		}
		if field.IsList() {
			if field.Kind() == protoreflect.MessageKind {
				list := message.Mutable(field).List()
				for item := 0; item < list.Len(); item++ {
					clearFieldByName(list.Get(item).Message(), name)
				}
			}
			continue
		}
		if field.Kind() == protoreflect.MessageKind {
			clearFieldByName(message.Mutable(field).Message(), name)
		}
	}
}
