package main

import (
	"net"
	"testing"
	"time"

	"github.com/josephjohncox/wallaby/pkg/connector"
)

func TestIggyExampleDisablesUnsupportedAutomaticDDL(t *testing.T) {
	t.Parallel()
	cfg, err := loadFlowConfigFile("../../examples/flows/postgres_to_iggy.yaml")
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Config.DDL.AutoApply == nil || *cfg.Config.DDL.AutoApply {
		t.Fatal("Iggy example must explicitly disable the shipped auto-apply DDL default")
	}
}

func TestIggyConnectivityUsesTypedAddressOnly(t *testing.T) {
	t.Parallel()
	if err := checkEndpointConnectivity(connector.EndpointIggy, map[string]string{"host": "localhost"}, time.Second); err == nil {
		t.Fatal("built-in Iggy fell back to custom connector connectivity aliases")
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	if err := checkEndpointConnectivity(connector.EndpointIggy, map[string]string{"address": listener.Addr().String()}, time.Second); err != nil {
		t.Fatal(err)
	}
}
