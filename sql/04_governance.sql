------------------------------------------------------------
-- 4. Governance: Masking, Tagging, Row Access Policies
------------------------------------------------------------

-- Masking Policy
CREATE OR REPLACE MASKING POLICY mask_financial_amount
    AS (val NUMBER(10,2))
    RETURNS NUMBER(10,2) ->
    CASE
        WHEN CURRENT_ROLE() IN ('FINANCE_ADMIN', 'ACCOUNTADMIN') THEN val
        ELSE -1.00
    END;

ALTER ICEBERG TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3"
    MODIFY COLUMN "amount" SET MASKING POLICY mask_financial_amount;

-- Verify
SELECT *
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    REF_ENTITY_NAME => 'glue_iceberg_db."your_glue_database"."customer_orders_v3"',
    REF_ENTITY_DOMAIN => 'TABLE'
));

-- Object Tagging (Horizon)
ALTER ICEBERG TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3"
    SET TAG SNOWFLAKE.CORE.SEMANTIC_CATEGORY = 'FINANCIAL';

SELECT SYSTEM$GET_TAG(
    'SNOWFLAKE.CORE.SEMANTIC_CATEGORY',
    'glue_iceberg_db."your_glue_database"."customer_orders_v3"',
    'TABLE'
);

-- Row Access Policy
CREATE OR REPLACE ROW ACCESS POLICY region_access_policy
    AS (region_val VARCHAR)
    RETURNS BOOLEAN ->
    CASE
        WHEN CURRENT_ROLE() = 'ACCOUNTADMIN' THEN TRUE
        WHEN CURRENT_ROLE() = 'US_ANALYST' AND region_val = 'US' THEN TRUE
        WHEN CURRENT_ROLE() = 'EU_ANALYST' AND region_val = 'EU' THEN TRUE
        ELSE FALSE
    END;

ALTER ICEBERG TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3"
    ADD ROW ACCESS POLICY region_access_policy ON ("region");
