------------------------------------------------------------
-- 5. Dynamic Iceberg Table
------------------------------------------------------------

CREATE DYNAMIC ICEBERG TABLE customer_orders_enriched (
    order_id NUMBER(10,0),
    product_name VARCHAR,
    amount NUMBER(10,2),
    region VARCHAR,
    order_month VARCHAR
)
  TARGET_LAG = '20 minutes'
  WAREHOUSE = compute_wh
  EXTERNAL_VOLUME = 's3_iceberg_volume'
  CATALOG = 'SNOWFLAKE'
  BASE_LOCATION = 'enriched/customer_orders'
  ICEBERG_VERSION = 3
AS
    SELECT
        "order_id",
        "product_name",
        "amount",
        "region",
        TO_CHAR("order_date", 'YYYY-MM') AS order_month
    FROM glue_iceberg_db."your_glue_database"."customer_orders_v3";
