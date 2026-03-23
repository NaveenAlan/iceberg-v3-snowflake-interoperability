------------------------------------------------------------
-- 3. Inspect Deletion Vectors
------------------------------------------------------------

SELECT
    FILE_NAME,
    CASE
        WHEN FILE_NAME LIKE '%-deletes.puffin' THEN 'DELETION_VECTOR'
        WHEN FILE_NAME LIKE '%.parquet' THEN 'DATA_FILE'
        ELSE 'OTHER'
    END AS file_type,
    FILE_SIZE,
    ROW_COUNT
FROM TABLE(
    INFORMATION_SCHEMA.ICEBERG_TABLE_FILES(
        'glue_iceberg_db."your_glue_database"."customer_orders_v3"'
    )
)
ORDER BY file_type, FILE_NAME;
