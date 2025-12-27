terraform {
  required_providers {
    ductstream = {
      source  = "josephjohncox/ductstream"
      version = "0.1.0"
    }
  }
}

provider "ductstream" {
  endpoint = "localhost:8080"
  insecure = true
}

resource "ductstream_flow" "pg_to_kafka" {
  name              = "pg_to_kafka"
  wire_format       = "arrow"
  start_immediately = true

  source {
    name = "pg-source"
    type = "postgres"
    options = {
      dsn             = "postgres://user:pass@localhost:5432/app?sslmode=disable"
      slot            = "ductstream_slot"
      publication     = "ductstream_pub"
      batch_size      = "500"
      batch_timeout   = "1s"
      status_interval = "10s"
      create_slot     = "true"
      format          = "arrow"
    }
  }

  destinations = [
    {
      name = "kafka-out"
      type = "kafka"
      options = {
        brokers     = "localhost:9092"
        topic       = "ductstream.cdc"
        format      = "arrow"
        compression = "lz4"
        acks        = "all"
      }
    }
  ]
}
