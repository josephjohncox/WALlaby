package checkpoint

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"reflect"
	"sort"
	"strconv"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

var timeType = reflect.TypeOf(time.Time{})

type canonicalValueEncoder struct {
	buffer bytes.Buffer
	seen   map[canonicalVisit]bool
}

type canonicalVisit struct {
	typ reflect.Type
	ptr uintptr
}

type canonicalMapEntry struct {
	key     reflect.Value
	encoded []byte
}

// hashOutboxBatch hashes concrete values, including their Go types, in stable
// map-key order. The checkpoint timestamp is delivery metadata rather than
// position identity, so replay of the same batch may carry a new timestamp.
func hashOutboxBatch(batch connector.Batch) (string, error) {
	batch.Checkpoint.Timestamp = time.Time{}
	encoder := canonicalValueEncoder{seen: make(map[canonicalVisit]bool)}
	if err := encoder.encode(reflect.ValueOf(batch)); err != nil {
		return "", err
	}
	digest := sha256.Sum256(encoder.buffer.Bytes())
	return hex.EncodeToString(digest[:]), nil
}

func (e *canonicalValueEncoder) encode(value reflect.Value) error {
	if !value.IsValid() {
		e.writeString("invalid")
		return nil
	}
	e.writeString(value.Kind().String())
	e.writeString(typeIdentity(value.Type()))

	if value.Type() == timeType {
		encoded, err := value.Interface().(time.Time).MarshalBinary()
		if err != nil {
			return fmt.Errorf("encode time: %w", err)
		}
		e.writeBytes(encoded)
		return nil
	}

	switch value.Kind() {
	case reflect.Interface:
		if value.IsNil() {
			e.writeString("nil")
			return nil
		}
		return e.encode(value.Elem())
	case reflect.Pointer:
		if value.IsNil() {
			e.writeString("nil")
			return nil
		}
		visit := canonicalVisit{typ: value.Type(), ptr: value.Pointer()}
		if e.seen[visit] {
			return fmt.Errorf("outbox batch contains a pointer cycle at %s", value.Type())
		}
		e.seen[visit] = true
		defer delete(e.seen, visit)
		return e.encode(value.Elem())
	case reflect.Bool:
		e.writeString(strconv.FormatBool(value.Bool()))
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		e.writeString(strconv.FormatInt(value.Int(), 10))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		e.writeString(strconv.FormatUint(value.Uint(), 10))
	case reflect.Float32:
		e.writeString(strconv.FormatUint(uint64(math.Float32bits(float32(value.Float()))), 16))
	case reflect.Float64:
		e.writeString(strconv.FormatUint(math.Float64bits(value.Float()), 16))
	case reflect.Complex64:
		complexValue := complex64(value.Complex())
		e.writeString(strconv.FormatUint(uint64(math.Float32bits(real(complexValue))), 16))
		e.writeString(strconv.FormatUint(uint64(math.Float32bits(imag(complexValue))), 16))
	case reflect.Complex128:
		complexValue := value.Complex()
		e.writeString(strconv.FormatUint(math.Float64bits(real(complexValue)), 16))
		e.writeString(strconv.FormatUint(math.Float64bits(imag(complexValue)), 16))
	case reflect.String:
		e.writeString(value.String())
	case reflect.Slice:
		if value.IsNil() {
			e.writeString("nil")
			return nil
		}
		fallthrough
	case reflect.Array:
		e.writeString(strconv.Itoa(value.Len()))
		for index := 0; index < value.Len(); index++ {
			if err := e.encode(value.Index(index)); err != nil {
				return err
			}
		}
	case reflect.Map:
		if value.IsNil() {
			e.writeString("nil")
			return nil
		}
		entries := make([]canonicalMapEntry, 0, value.Len())
		iterator := value.MapRange()
		for iterator.Next() {
			key := iterator.Key()
			keyEncoder := canonicalValueEncoder{seen: make(map[canonicalVisit]bool)}
			if err := keyEncoder.encode(key); err != nil {
				return fmt.Errorf("encode map key: %w", err)
			}
			entries = append(entries, canonicalMapEntry{key: key, encoded: keyEncoder.buffer.Bytes()})
		}
		sort.Slice(entries, func(left, right int) bool {
			return bytes.Compare(entries[left].encoded, entries[right].encoded) < 0
		})
		e.writeString(strconv.Itoa(len(entries)))
		for _, entry := range entries {
			e.writeBytes(entry.encoded)
			if err := e.encode(value.MapIndex(entry.key)); err != nil {
				return err
			}
		}
	case reflect.Struct:
		e.writeString(strconv.Itoa(value.NumField()))
		for index := 0; index < value.NumField(); index++ {
			e.writeString(value.Type().Field(index).Name)
			if err := e.encode(value.Field(index)); err != nil {
				return err
			}
		}
	case reflect.Chan, reflect.Func, reflect.UnsafePointer:
		return fmt.Errorf("outbox batch contains unsupported %s value of type %s", value.Kind(), value.Type())
	default:
		return fmt.Errorf("outbox batch contains unsupported %s value of type %s", value.Kind(), value.Type())
	}
	return nil
}

func typeIdentity(value reflect.Type) string {
	return value.PkgPath() + ":" + value.String()
}

func (e *canonicalValueEncoder) writeString(value string) {
	e.writeBytes([]byte(value))
}

func (e *canonicalValueEncoder) writeBytes(value []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(value)))
	_, _ = e.buffer.Write(size[:])
	_, _ = e.buffer.Write(value)
}
