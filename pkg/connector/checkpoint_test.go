package connector

import (
	"errors"
	"testing"
)

func TestCompareCheckpointLSN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		left    string
		right   string
		want    int
		wantErr error
	}{
		{name: "postgres less", left: "0/16B6C50", right: "0/16B6C51", want: -1},
		{name: "postgres high word", left: "1/0", right: "0/FFFFFFFF", want: 1},
		{name: "postgres equal case insensitive", left: "A/ff", right: "a/FF", want: 0},
		{name: "ordinal less", left: "9", right: "10", want: -1},
		{name: "large ordinal", left: "184467440737095516160", right: "184467440737095516159", want: 1},
		{name: "mixed kinds", left: "1", right: "0/1", wantErr: ErrCheckpointPosition},
		{name: "invalid", left: "not-an-lsn", right: "1", wantErr: ErrCheckpointPosition},
		{name: "whitespace rejected", left: " 9 ", right: "9", wantErr: ErrCheckpointPosition},
		{name: "signed rejected", left: "+9", right: "9", wantErr: ErrCheckpointPosition},
		{name: "empty", left: "", right: "1", wantErr: ErrCheckpointPosition},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			got, err := CompareCheckpointLSN(tt.left, tt.right)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("CompareCheckpointLSN() error = %v, want %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("CompareCheckpointLSN() error = %v", err)
			}
			if got != tt.want {
				t.Fatalf("CompareCheckpointLSN() = %d, want %d", got, tt.want)
			}
		})
	}
}

func TestCanonicalizeCheckpointPosition(t *testing.T) {
	t.Parallel()
	tests := []struct {
		raw     string
		want    string
		wantErr bool
	}{
		{raw: "0009", want: "9"},
		{raw: "000a/000ff", want: "A/FF"},
		{raw: "A/fF", want: "A/FF"},
		{raw: " 9", wantErr: true},
		{raw: "9 ", wantErr: true},
		{raw: "+9", wantErr: true},
		{raw: "-9", wantErr: true},
		{raw: "1/2/3", wantErr: true},
		{raw: "G/1", wantErr: true},
	}
	for _, test := range tests {
		test := test
		t.Run(test.raw, func(t *testing.T) {
			t.Parallel()
			got, err := CanonicalizeCheckpointPosition(test.raw)
			if test.wantErr {
				if !errors.Is(err, ErrCheckpointPosition) {
					t.Fatalf("CanonicalizeCheckpointPosition(%q) error = %v", test.raw, err)
				}
				return
			}
			if err != nil || got != test.want {
				t.Fatalf("CanonicalizeCheckpointPosition(%q) = %q, %v; want %q", test.raw, got, err, test.want)
			}
		})
	}
}

func TestCheckpointPositionIDStableMetadataIdentity(t *testing.T) {
	t.Parallel()
	first := Checkpoint{Metadata: map[string]string{
		"mode": "backfill", "table": "public.accounts", "partition": "1/4", "cursor": "42", "done": "true",
	}}
	reordered := Checkpoint{Metadata: map[string]string{
		"done": "true", "cursor": "42", "partition": "1/4", "table": "public.accounts", "mode": "backfill",
	}}
	firstID, err := CheckpointPositionID(first)
	if err != nil {
		t.Fatal(err)
	}
	reorderedID, err := CheckpointPositionID(reordered)
	if err != nil {
		t.Fatal(err)
	}
	if firstID != reorderedID {
		t.Fatalf("stable identities differ: %q != %q", firstID, reorderedID)
	}
	second := first
	second.Metadata = map[string]string{}
	for key, value := range first.Metadata {
		second.Metadata[key] = value
	}
	second.Metadata["cursor"] = "43"
	secondID, err := CheckpointPositionID(second)
	if err != nil {
		t.Fatal(err)
	}
	if secondID == firstID {
		t.Fatalf("distinct cursors shared identity %q", firstID)
	}
}
