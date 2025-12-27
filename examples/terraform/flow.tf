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

resource "ductstream_flow" "pg_to_s3" {
  name              = "pg_to_s3"
  wire_format       = "parquet"
  start_immediately = true

  source {
    name = "pg-source"
    type = "postgres"
    options = {
      dsn             = "postgres://user:pass@localhost:5432/app?sslmode=disable"
      slot            = "ductstream_slot"
      publication     = "ductstream_pub"
      batch_size      = "1000"
      batch_timeout   = "2s"
      status_interval = "10s"
      create_slot     = "true"
      format          = "parquet"
    }
  }

  destinations = [
    {
      name = "s3-out"
      type = "s3"
      options = {
        bucket      = "my-ductstream-bucket"
        prefix      = "cdc/"
        region      = "us-east-1"
        format      = "parquet"
        compression = "gzip"
      }
    }
  ]
}
