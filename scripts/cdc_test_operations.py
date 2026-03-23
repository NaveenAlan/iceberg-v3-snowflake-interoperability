from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CDCTestOperations") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://your-iceberg-data/glue_tables") \
    .getOrCreate()

TABLE = "glue_catalog.your_glue_database.customer_orders_v3"

spark.sql(f"""
    INSERT INTO {TABLE} VALUES
    (4, 104, DATE '2025-01-20', 'Widget D', 450.00, 'LATAM'),
    (5, 105, DATE '2025-01-21', 'Widget E', 550.00, 'EMEA'),
    (6, 106, DATE '2025-01-22', 'Widget F', 650.00, 'APAC')
""")

spark.sql(f"""
    UPDATE {TABLE}
    SET amount = 299.99, product_name = 'Widget B Pro'
    WHERE order_id = 2
""")

spark.sql(f"""
    DELETE FROM {TABLE}
    WHERE order_id = 4
""")

spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN loyalty_tier STRING")

print("CDC test operations complete.")
spark.stop()
