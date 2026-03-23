from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CreateV3TableWithRowLineage") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-iceberg-data/glue_tables") \
    .getOrCreate()

spark.sql("""
    CREATE TABLE glue_catalog.your_glue_database.customer_orders_v3 (
        order_id INT,
        customer_id INT,
        order_date DATE,
        product_name STRING,
        amount DECIMAL(10,2),
        region STRING
    )
    USING iceberg
    TBLPROPERTIES (
        'format-version' = '3',
        'write.delete.mode' = 'merge-on-read',
        'write.update.mode' = 'merge-on-read',
        'write.merge.mode' = 'merge-on-read'
    )
""")

spark.sql("""
    INSERT INTO glue_catalog.your_glue_database.customer_orders_v3 VALUES
    (1, 101, DATE '2025-01-15', 'Widget A', 150.00, 'US'),
    (2, 102, DATE '2025-01-16', 'Widget B', 250.00, 'EU'),
    (3, 103, DATE '2025-01-17', 'Widget C', 350.00, 'APAC')
""")

print("V3 table created with row lineage enabled.")
spark.stop()
