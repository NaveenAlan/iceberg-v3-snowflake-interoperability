------------------------------------------------------------
-- 2. Create Standard Stream & Query CDC
------------------------------------------------------------

SHOW ICEBERG TABLES IN DATABASE glue_iceberg_db;

SHOW PARAMETERS LIKE 'ICEBERG_VERSION'
  IN TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3";

SELECT * FROM glue_iceberg_db."your_glue_database"."customer_orders_v3" LIMIT 10;

CREATE OR REPLACE STREAM cdc_stream
    ON TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3";

SHOW STREAMS LIKE 'CDC_STREAM';

SELECT
    "order_id",
    "product_name",
    "amount",
    "region",
    METADATA$ACTION,
    METADATA$ISUPDATE,
    METADATA$ROW_ID
FROM cdc_stream
ORDER BY "order_id", METADATA$ACTION DESC;
