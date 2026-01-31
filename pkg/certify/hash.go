package certify

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"reflect"
	"sort"
	"strconv"
)

type jsonNumber string

func writeString(h hash.Hash, value string) {
	length := strconv.Itoa(len(value))
	_, _ = h.Write([]byte(length))
	_, _ = h.Write([]byte{':'})
	if value != "" {
		_, _ = h.Write([]byte(value))
	}
	_, _ = h.Write([]byte{0})
}

func writeBytes(h hash.Hash, value []byte) {
	length := strconv.Itoa(len(value))
	_, _ = h.Write([]byte(length))
	_, _ = h.Write([]byte{':'})
	if len(value) > 0 {
		_, _ = h.Write(value)
	}
	_, _ = h.Write([]byte{0})
}

func hashMap(h hash.Hash, value map[string]any) error {
	keys := make([]string, 0, len(value))
	for key := range value {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeString(h, key)
		if err := hashValue(h, value[key]); err != nil {
			return err
		}
	}
	return nil
}

func hashReflect(h hash.Hash, value any) error {
	rv := reflect.ValueOf(value)
	switch rv.Kind() {
	case reflect.Slice, reflect.Array:
		if rv.Type().Elem().Kind() == reflect.Uint8 {
			return hashValue(h, rv.Bytes())
		}
		writeString(h, "slice")
		for i := 0; i < rv.Len(); i++ {
			if err := hashValue(h, rv.Index(i).Interface()); err != nil {
				return err
			}
		}
		return nil
	case reflect.Map:
		writeString(h, "map")
		keys := rv.MapKeys()
		if len(keys) == 0 {
			return nil
		}
		type keyPair struct {
			keyString string
			value     reflect.Value
		}
		ordered := make([]keyPair, 0, len(keys))
		for _, key := range keys {
			ordered = append(ordered, keyPair{
				keyString: fmt.Sprint(key.Interface()),
				value:     key,
			})
		}
		sort.Slice(ordered, func(i, j int) bool {
			return ordered[i].keyString < ordered[j].keyString
		})
		for _, pair := range ordered {
			writeString(h, pair.keyString)
			val := rv.MapIndex(pair.value)
			if !val.IsValid() {
				writeString(h, "nil")
				continue
			}
			if err := hashValue(h, val.Interface()); err != nil {
				return err
			}
		}
		return nil
	case reflect.Struct:
		if rv.CanConvert(reflect.TypeOf(json.Number(""))) {
			num := rv.Convert(reflect.TypeOf(json.Number(""))).Interface().(json.Number)
			return hashValue(h, jsonNumber(num))
		}
	}

	switch v := value.(type) {
	case json.RawMessage:
		if len(v) == 0 {
			return hashValue(h, nil)
		}
		var decoded any
		if err := json.Unmarshal(v, &decoded); err != nil {
			writeString(h, "raw")
			writeString(h, hex.EncodeToString(v))
			return nil //nolint:nilerr // fallback to raw JSON bytes when decoding fails
		}
		return hashValue(h, decoded)
	case json.Number:
		return hashValue(h, jsonNumber(v))
	}

	writeString(h, "fmt")
	writeString(h, fmt.Sprint(value))
	return nil
}

func strconvFormatInt(value int64) string {
	return strconv.FormatInt(value, 10)
}

func strconvFormatUint(value uint64) string {
	return strconv.FormatUint(value, 10)
}

func strconvFormatFloat(value float64) string {
	return strconv.FormatFloat(value, 'g', -1, 64)
}
