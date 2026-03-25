------------------------------------------------------------
-- 7. CLD Write-Back: Create & Populate Tables in External Catalog from Snowflake
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

SELECT * FROM "your_database"."orders_summary";

SELECT SYSTEM$GET_ICEBERG_TABLE_INFORMATION(
    'glue_iceberg_db."your_database"."orders_summary"'
);
