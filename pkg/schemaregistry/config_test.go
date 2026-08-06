package schemaregistry

import (
	"strings"
	"testing"
	"time"
)

func TestConfigFromOptionsStrictTypedValues(t *testing.T) {
	cfg, err := ConfigFromOptions(map[string]string{
		OptRegistryURL:            " https://registry.example ",
		OptRegistryPassword:       "  exact secret  ",
		OptRegistryTimeout:        " 3s ",
		OptRegistryApicurioCompat: "false",
	})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Type != "csr" || cfg.URL != "https://registry.example" || cfg.Password != "  exact secret  " || cfg.Timeout != 3*time.Second || cfg.ApicurioCompat {
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
