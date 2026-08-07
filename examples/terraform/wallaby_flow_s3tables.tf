resource "wallaby_flow" "postgres_to_iceberg_s3tables" {
  name              = "postgres_to_iceberg_s3tables"
  wire_format       = "parquet"
  start_immediately = false

  source = {
    name = "pg-source-s3tables"
    postgres_source = {
      mode = "POSTGRES_SOURCE_MODE_CDC"
      connection = {
        dsn = "postgres://user:pass@localhost:5432/app?sslmode=disable"
      }
      slot               = "wallaby_s3tables_slot"
      publication        = "wallaby_s3tables_pub"
      publication_tables = ["public.events"]
      managed            = true
      bootstrap          = "BOOTSTRAP_MODE_NEVER"
      format             = "WIRE_FORMAT_PARQUET"
    }
  }

  destinations = [
    {
      name = "iceberg-s3tables"
      iceberg = {
        catalog_profile         = "ICEBERG_CATALOG_PROFILE_S3_TABLES"
        control_table           = "__wallaby_control"
        destination_revision_id = "iceberg-s3tables-v1"
      }
    }
  ]

  config = {
    ack_policy = "materialized"

    materialization = {
      projection_id = "canonical_cdc_parquet_v2"
    }

    table_mappings = {
      version = 2
      destinations = [
        {
          destination = "iceberg-s3tables"
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
