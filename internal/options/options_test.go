package options

import (
	"math"
	"strings"
	"testing"
	"time"
)

func TestDecoderDefaultsAndStringForms(t *testing.T) {
	decoder := NewDecoder("destination", map[string]string{
		"raw":     "  keep me  ",
		"trimmed": "  value  ",
	})
	if got := decoder.Raw("raw", "fallback"); got != "  keep me  " {
		t.Fatalf("Raw() = %q", got)
	}
	if got := decoder.String("trimmed", "fallback"); got != "value" {
		t.Fatalf("String() = %q", got)
	}
	if got := decoder.String("missing", "fallback"); got != "fallback" {
		t.Fatalf("String() default = %q", got)
	}
	if got := decoder.Bool("bool", true); !got {
		t.Fatal("Bool() did not use default")
	}
	if got := decoder.Int("int", 7); got != 7 {
		t.Fatalf("Int() default = %d", got)
	}
	if got := decoder.Float64("float", 2.5); got != 2.5 {
		t.Fatalf("Float64() default = %v", got)
	}
	if got := decoder.Duration("duration", time.Second); got != time.Second {
		t.Fatalf("Duration() default = %v", got)
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}
}

func TestDecoderParsesTypedValues(t *testing.T) {
	decoder := NewDecoder("destination", map[string]string{
		"bool":     " true ",
		"int":      " 42 ",
		"float":    " 1.25 ",
		"duration": " 250ms ",
	})
	if !decoder.Bool("bool", false) || decoder.Int("int", 0) != 42 || decoder.Float64("float", 0) != 1.25 || decoder.Duration("duration", 0) != 250*time.Millisecond {
		t.Fatal("typed values were not decoded")
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("Err() = %v", err)
	}
}

func TestDecoderFloat64RejectsNonFiniteValues(t *testing.T) {
	for _, raw := range []string{"NaN", "+Inf", "-Inf"} {
		t.Run(raw, func(t *testing.T) {
			decoder := NewDecoder("http options", map[string]string{"backoff_factor": raw})
			if got := decoder.Float64("backoff_factor", 2); got != 2 {
				t.Fatalf("Float64(%q) = %v", raw, got)
			}
			if err := decoder.Err(); err == nil || !strings.Contains(err.Error(), "http options.backoff_factor") || !strings.Contains(err.Error(), "finite") {
				t.Fatalf("Float64(%q) error = %v", raw, err)
			}
		})
	}
	decoder := NewDecoder("http options", map[string]string{"backoff_factor": "1.7976931348623157e308"})
	if got := decoder.Float64("backoff_factor", 0); got != math.MaxFloat64 {
		t.Fatalf("Float64(MaxFloat64) = %v", got)
	}
	if err := decoder.Err(); err != nil {
		t.Fatalf("Float64(MaxFloat64) error = %v", err)
	}
}

func TestDecoderAliasedEnum(t *testing.T) {
	aliases := map[string]string{"": "wire", "wire": "wire", "record": "record_json", "record_json": "record_json", "raw": "record_json", "wal": "wal"}
	for raw, want := range map[string]string{"": "wire", " WIRE ": "wire", "record": "record_json", "RAW": "record_json", "wal": "wal"} {
		decoder := NewDecoder("destination", map[string]string{"payload_mode": raw})
		if got := decoder.AliasedEnum("payload_mode", "wire", aliases); got != want {
			t.Errorf("AliasedEnum(%q) = %q, want %q", raw, got, want)
		}
		if err := decoder.Err(); err != nil {
			t.Errorf("AliasedEnum(%q) error = %v", raw, err)
		}
	}
	decoder := NewDecoder("destination", map[string]string{"payload_mode": "unknown"})
	if got := decoder.AliasedEnum("payload_mode", "wire", aliases); got != "wire" {
		t.Fatalf("invalid AliasedEnum() = %q", got)
	}
	if err := decoder.Err(); err == nil || !strings.Contains(err.Error(), "destination.payload_mode") {
		t.Fatalf("AliasedEnum() error = %v", err)
	}
}

func TestDecoderMalformedPresentValuesAccumulate(t *testing.T) {
	decoder := NewDecoder("grpc options", map[string]string{
		"insecure":    "",
		"max_retries": "many",
		"factor":      "twice",
		"timeout":     "soon",
	})
	_ = decoder.Bool("insecure", true)
	_ = decoder.Int("max_retries", 3)
	_ = decoder.Float64("factor", 2)
	_ = decoder.Duration("timeout", time.Second)
	err := decoder.Err()
	if err == nil {
		t.Fatal("Err() = nil")
	}
	for _, key := range []string{"grpc options.insecure", "grpc options.max_retries", "grpc options.factor", "grpc options.timeout"} {
		if !strings.Contains(err.Error(), key) {
			t.Errorf("Err() = %q, missing %q", err, key)
		}
	}
}

func TestParseKeyValueListQuotedCommasAndColons(t *testing.T) {
	got, err := ParseKeyValueList(`Authorization: Bearer token,"X-List: alpha,beta","X-Colon: one:two"`)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"Authorization": "Bearer token",
		"X-List":        "alpha,beta",
		"X-Colon":       "one:two",
	}
	if len(got) != len(want) {
		t.Fatalf("ParseKeyValueList() = %#v", got)
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("ParseKeyValueList()[%q] = %q, want %q", key, got[key], value)
		}
	}
}

func TestParseCaseInsensitiveKeyValueListPreservesArbitraryValues(t *testing.T) {
	binaryValue := string([]byte{0, 0xff, 'x'})
	got, err := ParseCaseInsensitiveKeyValueList("Trace-Bin:" + binaryValue)
	if err != nil {
		t.Fatal(err)
	}
	if got["trace-bin"] != binaryValue {
		t.Fatalf("binary value = %v, want %v", []byte(got["trace-bin"]), []byte(binaryValue))
	}
	if _, err := ParseCaseInsensitiveKeyValueList("X-Test:one,x-test:two"); err == nil || !strings.Contains(err.Error(), "case normalization") {
		t.Fatalf("case collision error = %v", err)
	}
}

func TestParseHeaderListCanonicalizesAndValidates(t *testing.T) {
	got, err := ParseHeaderList(`Authorization: Bearer token,"X-List: alpha,beta","X-Colon: one:two"`)
	if err != nil {
		t.Fatal(err)
	}
	if got["authorization"] != "Bearer token" || got["x-list"] != "alpha,beta" || got["x-colon"] != "one:two" {
		t.Fatalf("ParseHeaderList() = %#v", got)
	}
	for _, test := range []struct {
		name string
		raw  string
		want string
	}{
		{name: "case collision", raw: "X-Test:one,x-test:two", want: "case normalization"},
		{name: "invalid name", raw: "Bad Name:value", want: "invalid header name"},
		{name: "invalid value", raw: "X-Test:value\x00bad", want: "invalid value"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseHeaderList(test.raw)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ParseHeaderList() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestParseKeyValueListRejectsInvalidInput(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "duplicate", raw: "X:one,X:two", want: `duplicate key "X"`},
		{name: "missing colon", raw: "X:one,bad", want: "missing ':'"},
		{name: "empty key", raw: " :value", want: "empty key"},
		{name: "empty item", raw: "X:one,", want: "is empty"},
		{name: "malformed CSV", raw: `"X:unterminated`, want: "read CSV"},
		{name: "multiple records", raw: "X:one\nY:two", want: "multiple CSV records"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseKeyValueList(test.raw)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ParseKeyValueList() error = %v, want %q", err, test.want)
			}
		})
	}
}
