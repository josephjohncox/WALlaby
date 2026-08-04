# Mapped Iceberg flows require durable destination-scoped table_mappings and
# canonical_cdc_parquet_v2. The Terraform provider does not expose rich table
# mappings until its dedicated follow-up task, so this directory intentionally
# does not publish an invalid wallaby_flow resource with legacy namespace,
# table_prefix, or canonical_cdc_parquet_v1 options.
#
# Use examples/flows/postgres_to_iceberg_s3tables.json through the API after
# replacing its source identity placeholders. Provision the versioned canonical
# S3 bucket, S3 Tables/Glue integration, IAM/Lake Formation grants, and
# Snowflake read-only catalog link separately as documented in
# docs/guides/s3-tables-snowflake.md.
