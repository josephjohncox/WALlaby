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

  source {
    name = "pg-source"
    type = "postgres"
    options = {
      dsn             = "postgres://user:pass@localhost:5432/app?sslmode=disable"
      slot            = "wallaby_slot"
      publication     = "wallaby_pub"
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
        topic       = "wallaby.cdc"
        format      = "arrow"
        compression = "lz4"
        acks        = "all"
      }
    }
  ]
}
