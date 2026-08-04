-- Read-only, zero-copy Snowflake access to WALlaby Iceberg tables in Amazon S3 Tables.
-- Replace every ${...} placeholder before execution. Run as a role with CREATE
-- INTEGRATION and CREATE DATABASE. Snowflake is an observer, never WALlaby's
-- checkpoint or delivery authority.

CREATE CATALOG INTEGRATION WALLABY_S3TABLES_CATALOG
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = '${S3TABLES_NAMESPACE}'
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.${AWS_REGION}.amazonaws.com/iceberg'
    CATALOG_API_TYPE = AWS_GLUE
    -- Supply the exact Glue REST catalog name exposed by the account's S3
    -- Tables integration. Snowflake's current S3 Tables example uses
    -- <account>:S3tablescatalog/<table-bucket>; verify rather than infer it.
    CATALOG_NAME = '${S3TABLES_GLUE_CATALOG_NAME}'
    ACCESS_DELEGATION_MODE = VENDED_CREDENTIALS
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = '${SNOWFLAKE_CATALOG_IAM_ROLE_ARN}'
    SIGV4_SIGNING_REGION = '${AWS_REGION}'
  )
  ENABLED = TRUE
  REFRESH_INTERVAL_SECONDS = 30;

-- Record GLUE_AWS_IAM_USER_ARN and GLUE_AWS_EXTERNAL_ID. Update the IAM role
-- trust policy with both values before creating the linked database. Replacing
-- this integration changes the external ID unless it is explicitly controlled.
DESCRIBE CATALOG INTEGRATION WALLABY_S3TABLES_CATALOG;

CREATE DATABASE WALLABY_S3TABLES
  LINKED_CATALOG = (
    CATALOG = 'WALLABY_S3TABLES_CATALOG'
    ALLOWED_NAMESPACES = ('${S3TABLES_NAMESPACE}')
    ALLOWED_WRITE_OPERATIONS = NONE
    SYNC_INTERVAL_SECONDS = 30
  )
  CATALOG_CASE_SENSITIVITY = CASE_INSENSITIVE;

SELECT SYSTEM$GET_CATALOG_LINKED_DATABASE_CONFIG('WALLABY_S3TABLES');
SELECT SYSTEM$CATALOG_LINK_STATUS('WALLABY_S3TABLES');
SHOW ICEBERG TABLES IN DATABASE WALLABY_S3TABLES;

-- WALlaby tables are append-only CDC changelogs, not current-state mirrors.
-- Example inspection after catalog discovery:
SELECT
  __wallaby_logical_batch_id,
  __wallaby_record_ordinal,
  __op,
  *
FROM WALLABY_S3TABLES.${S3TABLES_NAMESPACE}.CDC_${SOURCE_TABLE}
ORDER BY __wallaby_logical_batch_id, __wallaby_record_ordinal;
