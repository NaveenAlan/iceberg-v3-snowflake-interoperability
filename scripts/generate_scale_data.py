import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr, rand, lit, date_add, col, concat

spark = SparkSession.builder \
    .appName("V3DeletionVectorScaleTest") \
    .config("spark.sql.catalog.glue_catalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.glue_catalog.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog") \
    .config("spark.sql.catalog.glue_catalog.warehouse", "s3://nt-iceberg-sample-data/glue_tables") \
    .getOrCreate()

TABLE = "glue_catalog.naveen_iceberg_demo.orders_scale_10m"

spark.sql(f"DROP TABLE IF EXISTS {TABLE}")

spark.sql(f"""
    CREATE TABLE {TABLE} (
        order_id BIGINT,
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

print("Table created. Generating 10M rows...")

df = spark.range(0, 10_000_000) \
    .withColumn("order_id", col("id")) \
    .withColumn("customer_id", (rand() * 100000).cast("int")) \
    .withColumn("order_date", date_add(lit("2024-01-01"), (rand() * 365).cast("int"))) \
    .withColumn("product_name", concat(lit("Product-"), (col("id") % 500).cast("string"))) \
    .withColumn("amount", (rand() * 1000 + 10).cast("decimal(10,2)")) \
    .withColumn("region", expr("""
        CASE
            WHEN rand() < 0.25 THEN 'US-EAST'
            WHEN rand() < 0.50 THEN 'US-WEST'
            WHEN rand() < 0.75 THEN 'EU-WEST'
            ELSE 'AP-SOUTH'
        END
    """)) \
    .drop("id")

df.writeTo(TABLE).append()
print("10M rows inserted.")

print("Running UPDATE on 1000 rows to generate deletion vectors...")
spark.sql(f"""
    UPDATE {TABLE}
    SET amount = amount * 1.1, product_name = concat(product_name, '-UPDATED')
    WHERE order_id BETWEEN 5000 AND 5999
""")
print("UPDATE complete. Deletion vectors should now exist.")

row_count = spark.sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}").collect()[0]["cnt"]
print(f"Final row count: {row_count}")

spark.stop()
