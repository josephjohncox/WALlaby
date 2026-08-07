package schemaregistry

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestConfigFromOptionsStrictTypedValues(t *testing.T) {
	cfg, err := ConfigFromOptions(map[string]string{
		OptRegistryType:           "csr",
		OptRegistryURL:            " https://registry.example ",
		OptRegistryPassword:       "  exact secret  ",
		OptRegistryLocalDirectory: " /var/lib/wallaby/registry ",
		OptRegistryTimeout:        " 3s ",
		OptRegistryApicurioCompat: "false",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Type != "csr" || cfg.URL != "https://registry.example" || cfg.Password != "  exact secret  " || cfg.LocalDirectory != "/var/lib/wallaby/registry" || cfg.Timeout != 3*time.Second || cfg.ApicurioCompat {
		t.Fatalf("ConfigFromOptions() = %+v", cfg)
	}
}

func TestConfigFromOptionsDefaults(t *testing.T) {
	cfg, err := ConfigFromOptions(nil)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Timeout != 0 || !cfg.ApicurioCompat {
		t.Fatalf("ConfigFromOptions() defaults = %+v", cfg)
	}
}

func TestNewRegistryRejectsRemovedAliases(t *testing.T) {
	t.Parallel()
	for _, alias := range []string{"disabled", "confluent", "memory", "mem", "db"} {
		t.Run(alias, func(t *testing.T) {
			if _, err := NewRegistry(context.Background(), Config{Type: alias}); err == nil || !strings.Contains(err.Error(), "unsupported schema registry type") {
				t.Fatalf("NewRegistry(%q) error=%v, want unsupported type", alias, err)
			}
		})
	}
}

func TestConfigFromOptionsRejectsMalformedPresentTypedValues(t *testing.T) {
	_, err := ConfigFromOptions(map[string]string{
		OptRegistryTimeout:        "soon",
		OptRegistryApicurioCompat: "yes",
	})
	if err == nil {
		t.Fatal("ConfigFromOptions() error = nil")
	}
	for _, key := range []string{OptRegistryTimeout, OptRegistryApicurioCompat} {
		if !strings.Contains(err.Error(), "schema registry options."+key) {
			t.Errorf("ConfigFromOptions() error = %q, missing %q", err, key)
		}
	}
}
