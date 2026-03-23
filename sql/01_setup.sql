------------------------------------------------------------
-- 1. Infrastructure Setup
------------------------------------------------------------

CREATE OR REPLACE EXTERNAL VOLUME s3_iceberg_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'iceberg-s3'
      STORAGE_BASE_URL = 's3://your-iceberg-data/'
      STORAGE_PROVIDER = 'S3'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-iceberg-role'
    )
  )
  ALLOW_WRITES = TRUE;

CREATE OR REPLACE CATALOG INTEGRATION glue_rest_catalog
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'your_glue_database'
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.<REGION>.amazonaws.com/iceberg'
    WAREHOUSE = 'your_glue_database'
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-glue-rest-role'
    SIGV4_SIGNING_REGION = '<REGION>'
  )
  ENABLED = TRUE;

CREATE DATABASE glue_iceberg_db
  LINKED_CATALOG = (
    CATALOG = 'glue_rest_catalog'
    EXTERNAL_VOLUME = 's3_iceberg_volume'
    AUTO_REFRESH = TRUE
  );
