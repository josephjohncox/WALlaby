package postgres

import (
	"encoding/json"

	"github.com/jackc/pgx/v5/pgtype"
)

// RegisterRawJSONCodecs configures JSON/JSONB to return raw JSON bytes to preserve fidelity.
func RegisterRawJSONCodecs(m *pgtype.Map) {
	if m == nil {
		return
	}
	m.RegisterType(&pgtype.Type{
		Name:  "json",
		OID:   pgtype.JSONOID,
		Codec: &pgtype.JSONCodec{Marshal: json.Marshal, Unmarshal: rawJSONUnmarshal},
	})
	m.RegisterType(&pgtype.Type{
		Name:  "jsonb",
		OID:   pgtype.JSONBOID,
		Codec: &pgtype.JSONBCodec{Marshal: json.Marshal, Unmarshal: rawJSONUnmarshal},
	})
}

func rawJSONUnmarshal(src []byte, dst any) error {
	switch target := dst.(type) {
	case *any:
		if src == nil {
			*target = nil
			return nil
		}
		raw := make([]byte, len(src))
		copy(raw, src)
		*target = json.RawMessage(raw)
		return nil
	case *json.RawMessage:
		if src == nil {
			*target = nil
			return nil
		}
		raw := make([]byte, len(src))
		copy(raw, src)
		*target = raw
		return nil
	case *[]byte:
		if src == nil {
			*target = nil
			return nil
		}
		raw := make([]byte, len(src))
		copy(raw, src)
		*target = raw
		return nil
	case *string:
		if src == nil {
			*target = ""
			return nil
		}
		*target = string(src)
		return nil
	default:
		return json.Unmarshal(src, dst)
	}
}
