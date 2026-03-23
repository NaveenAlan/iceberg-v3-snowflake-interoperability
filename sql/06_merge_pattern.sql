------------------------------------------------------------
-- 6. Consume CDC Stream via MERGE
------------------------------------------------------------

CREATE OR REPLACE TABLE customer_orders_target (
    order_id NUMBER(10,0) PRIMARY KEY,
    product_name VARCHAR,
    amount NUMBER(10,2),
    region VARCHAR,
    last_updated TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP()
);

MERGE INTO customer_orders_target t
USING (
    SELECT
        "order_id",
        "product_name",
        "amount",
        "region",
        METADATA$ACTION,
        METADATA$ISUPDATE
    FROM cdc_stream
) s
ON t.order_id = s."order_id"
WHEN MATCHED AND s.METADATA$ACTION = 'DELETE' AND s.METADATA$ISUPDATE = FALSE
    THEN DELETE
WHEN MATCHED AND s.METADATA$ACTION = 'INSERT' AND s.METADATA$ISUPDATE = TRUE
    THEN UPDATE SET
        t.product_name = s."product_name",
        t.amount = s."amount",
        t.region = s."region",
        t.last_updated = CURRENT_TIMESTAMP()
WHEN NOT MATCHED AND s.METADATA$ACTION = 'INSERT'
    THEN INSERT (order_id, product_name, amount, region)
    VALUES (s."order_id", s."product_name", s."amount", s."region");
