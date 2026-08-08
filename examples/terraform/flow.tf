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

resource "wallaby_flow" "pg_to_s3" {
  name              = "pg_to_s3"
  wire_format       = "parquet"
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
      batch_size         = 1000
      batch_timeout      = "2s"
      status_interval    = "10s"
      create_slot        = true
      format             = "WIRE_FORMAT_PARQUET"
    }
  }

  destinations = [
    {
      name = "s3-out"
      s3 = {
        bucket      = "my-wallaby-bucket"
        prefix      = "cdc/"
        region      = "us-east-1"
        format      = "WIRE_FORMAT_PARQUET"
        compression = "COMPRESSION_GZIP"
      }
    }
  ]

  config = {
    ack_policy = "all"

    table_mappings = {
      version = 2
      destinations = [
        {
          destination = "s3-out"
          future_tables = {
            action        = "include"
            target_schema = "{{ .Schema }}"
            target_table  = "{{ .Table }}"
            future_columns = {
              action        = "include"
              target_column = "{{ .Column }}"
            }
            write = {
              mode        = "append"
              key_columns = []
            }
          }
          tables = []
        }
      ]
    }
  }
}
