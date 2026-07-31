# WALlaby flow only. Provision the versioned canonical S3 bucket, S3 Tables
# table bucket/Glue integration, IAM/Lake Formation grants, and Snowflake
# read-only catalog link separately as documented in
# docs/guides/s3-tables-snowflake.md.

variable "postgres_dsn" {
  type      = string
  sensitive = true
}

resource "wallaby_flow" "postgres_to_s3tables" {
  name              = "postgres-to-s3tables"
  wire_format       = "parquet"
  start_immediately = true

  source = {
    name = "postgres-source"
    type = "postgres"
    options = {
      dsn                      = var.postgres_dsn
      slot                     = "wallaby_s3tables"
      publication              = "wallaby_s3tables"
      managed                  = "true"
      bootstrap                = "never"
      create_slot              = "false"
      ensure_state             = "false"
      ensure_publication       = "false"
      sync_publication         = "false"
      streaming_transactions   = "true"
      source_system_identifier = "REPLACE_WITH_PG_SYSTEM_IDENTIFIER"
      source_lineage_id        = "REPLACE_WITH_STABLE_SOURCE_LINEAGE"
      publication_revision     = "REPLACE_WITH_PUBLICATION_FINGERPRINT"
    }
  }

  destinations = [{
    name = "s3tables-lake"
    type = "iceberg"
    options = {
      catalog_profile         = "s3tables"
      destination_revision_id = "s3tables-lake-v1"
      namespace               = "wallaby"
      table_prefix            = "cdc_"
      control_table           = "__wallaby_control"
    }
  }]

  config = {
    ack_policy = "materialized"
    materialization = {
      projection_id = "canonical_cdc_parquet_v1"
    }
  }
}
