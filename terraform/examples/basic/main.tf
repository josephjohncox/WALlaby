terraform {
  required_providers {
    wallaby = {
      source  = "josephjohncox/wallaby"
      version = "0.1.0"
    }
  }
}

provider "wallaby" {
  endpoint = "localhost:8080"
  insecure = true
}

resource "wallaby_flow" "pg_to_kafka" {
  name              = "pg_to_kafka"
  wire_format       = "arrow"
  start_immediately = true

  source = {
    name = "pg-source"
    postgres_source = {
      mode = "POSTGRES_SOURCE_MODE_CDC"
      connection = {
        dsn = "postgres://user:pass@localhost:5432/app?sslmode=disable"
      }
      slot               = "wallaby_slot"
      publication        = "wallaby_pub"
      publication_tables = ["public.events"]
      batch_size         = 500
      batch_timeout      = "1s"
      status_interval    = "10s"
      create_slot        = true
      format             = "WIRE_FORMAT_ARROW"
    }
  }

  destinations = [
    {
      name = "kafka-out"
      kafka = {
        brokers     = ["localhost:9092"]
        topic       = "wallaby.cdc"
        format      = "WIRE_FORMAT_ARROW"
        compression = "COMPRESSION_LZ4"
        acks        = "KAFKA_ACKS_ALL"
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
          action        = "include"
          target_schema = "{{ .Schema }}"
          target_table  = "{{ .Table }}"
          future_columns = {
            action        = "include"
            target_column = "{{ .Column }}"
          }
          write = { mode = "append", key_columns = [] }
        }
        tables = []
      }]
    }
  }
}
