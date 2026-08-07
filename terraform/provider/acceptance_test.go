//go:build acceptance

package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/hashicorp/terraform-plugin-framework/providerserver"
	"github.com/hashicorp/terraform-plugin-go/tfprotov6"
	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
)

func TestAccFlowBasic(t *testing.T) {
	if os.Getenv("WALLABY_TF_ACC") == "" {
		t.Skip("set WALLABY_TF_ACC=1 to enable acceptance tests")
	}

	endpoint := os.Getenv("WALLABY_TF_ENDPOINT")
	if endpoint == "" {
		t.Skip("WALLABY_TF_ENDPOINT is required")
	}

	dsn := os.Getenv("WALLABY_TF_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("WALLABY_TF_POSTGRES_DSN is required")
	}

	brokers := os.Getenv("WALLABY_TF_KAFKA_BROKERS")
	if brokers == "" {
		t.Skip("WALLABY_TF_KAFKA_BROKERS is required")
	}

	topic := os.Getenv("WALLABY_TF_KAFKA_TOPIC")
	if topic == "" {
		t.Skip("WALLABY_TF_KAFKA_TOPIC is required")
	}

	insecure := os.Getenv("WALLABY_TF_INSECURE")
	if insecure == "" {
		insecure = "true"
	}

	config := fmt.Sprintf(`
provider "wallaby" {
  endpoint = "%s"
  insecure = %s
}

resource "wallaby_flow" "acc" {
  name              = "acc_flow"
  wire_format       = "arrow"
  start_immediately = true

  source = {
    name = "pg-source"
    postgres_source = {
      mode = "POSTGRES_SOURCE_MODE_CDC"
      connection = {
        dsn = "%s"
      }
      slot            = "wallaby_acc_slot"
      publication     = "wallaby_acc_pub"
      batch_size      = 100
      batch_timeout   = "1s"
      status_interval = "10s"
      create_slot     = true
      format          = "WIRE_FORMAT_ARROW"
    }
  }

  destinations = [
    {
      name = "kafka-out"
      kafka = {
        brokers = ["%s"]
        topic   = "%s"
        format  = "WIRE_FORMAT_ARROW"
        acks    = "KAFKA_ACKS_ALL"
      }
    }
  ]

  config = {
    ack_policy = "all"
    table_mappings = {
      version = 2
      destinations = [{
        destination = "kafka-out"
        future_tables = {
          action = "include"
          target_schema = "{{ .Schema }}"
          target_table = "{{ .Table }}"
          future_columns = {
            action = "include"
            target_column = "{{ .Column }}"
          }
          write = { mode = "append", key_columns = [] }
        }
        tables = []
      }]
    }
  }
}
`, endpoint, insecure, dsn, brokers, topic)

	resource.Test(t, resource.TestCase{
		ProtoV6ProviderFactories: map[string]func() (tfprotov6.ProviderServer, error){
			"wallaby": providerserver.NewProtocol6WithError(New("test")()),
		},
		Steps: []resource.TestStep{
			{
				Config: config,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("wallaby_flow.acc", "source.postgres_source.mode", "POSTGRES_SOURCE_MODE_CDC"),
					resource.TestCheckResourceAttr("wallaby_flow.acc", "destinations.0.kafka.acks", "KAFKA_ACKS_ALL"),
				),
			},
			{
				Config:       config,
				RefreshState: true,
				Check: resource.ComposeAggregateTestCheckFunc(
					resource.TestCheckResourceAttr("wallaby_flow.acc", "source.postgres_source.mode", "POSTGRES_SOURCE_MODE_CDC"),
				),
			},
			{
				ResourceName:            "wallaby_flow.acc",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"start_immediately"},
			},
		},
	})
}
