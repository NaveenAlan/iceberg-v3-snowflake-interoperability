------------------------------------------------------------
-- 7. CLD Write-Back: Create & Populate Tables in External Catalog from Snowflake
--
-- Proves CLD is bidirectional: Snowflake writes a new V3 table
-- into the Glue catalog. EMR/Spark can read it immediately.
------------------------------------------------------------

USE DATABASE glue_iceberg_db;

CREATE OR REPLACE ICEBERG TABLE "your_database"."orders_summary" (
    "region" VARCHAR,
    "total_orders" INT,
    "total_revenue" NUMBER(12,2)
)
ICEBERG_VERSION = 3;

INSERT INTO "your_database"."orders_summary"
SELECT "region", COUNT(*), SUM("amount")
FROM "your_database"."customer_orders_v3"
GROUP BY "region";

------------------------------------------------------------
-- Verify: data is correct
------------------------------------------------------------
SELECT * FROM "your_database"."orders_summary";
-- Expected: 4 rows (EU, APAC, EMEA, US)

------------------------------------------------------------
-- Verify: V3 format version
------------------------------------------------------------
SHOW PARAMETERS LIKE 'ICEBERG_VERSION'
    IN TABLE glue_iceberg_db."your_database"."orders_summary";
-- Expected: ICEBERG_VERSION = 3

------------------------------------------------------------
-- Verify: metadata registered in Glue + S3
------------------------------------------------------------
SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(
    'glue_iceberg_db."your_database"."orders_summary"'
);
-- Expected: valid metadataLocation in S3

------------------------------------------------------------
-- Verify: Parquet files written to S3
------------------------------------------------------------
SELECT FILE_NAME, FILE_SIZE, ROW_COUNT
FROM TABLE(INFORMATION_SCHEMA.ICEBERG_TABLE_FILES(
    'glue_iceberg_db."your_database"."orders_summary"'
));

------------------------------------------------------------
-- Verify: table shows as UNMANAGED (Glue-cataloged) with write capability
------------------------------------------------------------
SHOW ICEBERG TABLES LIKE 'orders_summary'
    IN SCHEMA glue_iceberg_db."your_database";
-- Expected: catalog_table_name = orders_summary, can_write_metadata = Y

------------------------------------------------------------
-- From EMR/Spark, confirm the table is readable via Glue:
--
--   spark.sql("SELECT * FROM glue_catalog.your_database.orders_summary").show()
--
------------------------------------------------------------
