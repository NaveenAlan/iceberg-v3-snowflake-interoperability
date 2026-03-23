# Code Companion: The Battle for Compute — Iceberg V3 & Snowflake Interoperability

> Full replication scripts for the Medium article.  
> Tested: **March 23, 2026** | Snowflake V3 Support: **Public Preview**  
> EMR: **7.12** (Spark 3.5.6)

---

## Prerequisites

### AWS Resources Required
- S3 bucket for Iceberg data (e.g., `s3://your-iceberg-data/`)
- AWS Glue catalog access
- EMR Serverless application (EMR 7.12+) — **Glue 5.0 does NOT write row lineage**
- IAM execution role with S3 + Glue permissions

### Snowflake Resources Required
- External Volume pointing to your S3 bucket
- Catalog Integration (REST-based for Glue)
- Catalog-Linked Database (CLD)

---

## 1. Snowflake Infrastructure Setup

### External Volume

```sql
CREATE OR REPLACE EXTERNAL VOLUME s3_iceberg_volume
  STORAGE_LOCATIONS = (
    (
      NAME = 'iceberg-s3'
      STORAGE_BASE_URL = 's3://your-iceberg-data/'
      STORAGE_PROVIDER = 'S3'
      STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-iceberg-role'
    )
  )
  ALLOW_WRITES = TRUE;
```

### Glue REST Catalog Integration (Vended Credentials)

```sql
CREATE OR REPLACE CATALOG INTEGRATION glue_rest_catalog
  CATALOG_SOURCE = ICEBERG_REST
  TABLE_FORMAT = ICEBERG
  CATALOG_NAMESPACE = 'your_glue_database'
  REST_CONFIG = (
    CATALOG_URI = 'https://glue.<REGION>.amazonaws.com/iceberg'
    WAREHOUSE = 'your_glue_database'
  )
  REST_AUTHENTICATION = (
    TYPE = SIGV4
    SIGV4_IAM_ROLE = 'arn:aws:iam::<ACCOUNT_ID>:role/snowflake-glue-rest-role'
    SIGV4_SIGNING_REGION = '<REGION>'
  )
  ENABLED = TRUE;
```

### Catalog-Linked Database

```sql
CREATE DATABASE glue_iceberg_db
  LINKED_CATALOG = (
    CATALOG = 'glue_rest_catalog'
    EXTERNAL_VOLUME = 's3_iceberg_volume'
    AUTO_REFRESH = TRUE
  );
```

> CLD auto-discovers all tables in the Glue namespace and syncs schema changes (ADD/DROP/RENAME columns) automatically.

---

## 2. EMR Spark: Create V3 Table with Row Lineage

### Spark Script (`create_v3_table.py`)

```python
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
```

### Submit to EMR Serverless

```bash
aws s3 cp create_v3_table.py s3://your-iceberg-data/scripts/

aws emr-serverless start-job-run \
    --application-id <EMR_APP_ID> \
    --execution-role-arn arn:aws:iam::<ACCOUNT_ID>:role/EMR-Execution-Role \
    --job-driver '{
        "sparkSubmit": {
            "entryPoint": "s3://your-iceberg-data/scripts/create_v3_table.py",
            "sparkSubmitParameters": "--conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.7.1,software.amazon.awssdk:bundle:2.29.38"
        }
    }' \
    --configuration-overrides '{
        "monitoringConfiguration": {
            "s3MonitoringConfiguration": {
                "logUri": "s3://your-iceberg-data/logs/"
            }
        }
    }'
```

### Verify Row Lineage in Metadata

```bash
aws s3 ls s3://your-iceberg-data/glue_tables/your_glue_database.db/customer_orders_v3/metadata/ \
    --recursive | sort | tail -1

aws s3 cp s3://your-iceberg-data/glue_tables/.../metadata/00001-xxx.metadata.json - \
    | python3 -m json.tool | grep next-row-id
```

Expected output:
```json
"next-row-id": 3
```

> If `next-row-id` is null or missing, row lineage is NOT enabled. Your engine does not support V3 row lineage.

---

## 3. EMR Spark: DML Operations for CDC Testing

### Spark Script (`cdc_test_operations.py`)

```python
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
```

---

## 4. Snowflake: Standard Stream on Externally Managed V3 Table

```sql
SHOW ICEBERG TABLES IN DATABASE glue_iceberg_db;

SHOW PARAMETERS LIKE 'ICEBERG_VERSION'
  IN TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3";

SELECT * FROM glue_iceberg_db."your_glue_database"."customer_orders_v3" LIMIT 10;

CREATE OR REPLACE STREAM cdc_stream
    ON TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3";

SHOW STREAMS LIKE 'CDC_STREAM';
-- Expected: mode = DEFAULT, type = DELTA
```

---

## 5. Snowflake: Query the CDC Stream

```sql
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
```

### Expected Results

| order_id | product_name | amount | METADATA$ACTION | METADATA$ISUPDATE | METADATA$ROW_ID |
|----------|-------------|--------|-----------------|-------------------|-----------------|
| 2 | Widget B | 250.00 | DELETE | true | 0000000000000001:1 |
| 2 | Widget B Pro | 299.99 | INSERT | true | 000000000000000F:15 |
| 5 | Widget E | 550.00 | INSERT | false | 0000000000000007:7 |
| 6 | Widget F | 650.00 | INSERT | false | 0000000000000008:8 |

**How to read this:**
- **INSERT** with `ISUPDATE = false` — a new row
- **DELETE + INSERT** with `ISUPDATE = true` — an UPDATE (old value deleted, new value inserted)
- **DELETE** with `ISUPDATE = false` — a true delete
- `METADATA$ROW_ID` — the V3 row lineage identifier

---

## 6. Deletion Vectors: Merge-on-Read in Action

V3 with `merge-on-read` mode uses **deletion vectors** (`.puffin` files) instead of rewriting entire data files. This is what makes CDC efficient at scale.

### How It Works

When you DELETE or UPDATE a row in merge-on-read mode:
1. The original data file is **not rewritten**
2. A small `.puffin` deletion vector file is created, marking which rows are logically deleted
3. Snowflake merges these at read time — fast writes, efficient CDC

### Inspecting Deletion Vectors

```sql
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
```

### Expected Output

| FILE_NAME | file_type | FILE_SIZE | ROW_COUNT |
|-----------|-----------|-----------|-----------|
| .../00000-4-...-00001.parquet | DATA_FILE | 1801 | 1 |
| .../00001-2-...-00001.parquet | DATA_FILE | 1754 | 1 |
| .../00002-3-...-00001.parquet | DATA_FILE | 1754 | 1 |
| ... | DATA_FILE | ... | ... |
| .../00000-4-...-00001-deletes.puffin | DELETION_VECTOR | 2184 | 0 |
| .../00000-5-...-00001-deletes.puffin | DELETION_VECTOR | 2183 | 0 |
| .../00000-6-...-00001-deletes.puffin | DELETION_VECTOR | 2270 | 0 |

Notice:
- **Data files** (~1.7KB each) contain the original row data
- **Deletion vectors** (~2.2KB `.puffin` files) are tiny markers — no data rewrite needed
- `ROW_COUNT = 0` on deletion vectors because they don't contain rows, just position markers

### Why This Matters for CDC

Without deletion vectors (copy-on-write), a single UPDATE on a 1GB Parquet file rewrites the entire file. With deletion vectors:
- Write: ~2KB puffin file (instant)
- Snowflake stream: reads the deletion vector + new data file = knows exactly what changed
- At scale (billions of rows), this is the difference between minutes and hours of CDC lag

---

## 7. Governing the Lake: RBAC & Horizon on Iceberg Tables

Snowflake governance features work identically on Iceberg tables — whether Snowflake-managed or externally managed via CLD.

### Masking Policies on Iceberg Tables

```sql
CREATE OR REPLACE MASKING POLICY mask_financial_amount
    AS (val NUMBER(10,2))
    RETURNS NUMBER(10,2) ->
    CASE
        WHEN CURRENT_ROLE() IN ('FINANCE_ADMIN', 'ACCOUNTADMIN') THEN val
        ELSE -1.00
    END;

ALTER ICEBERG TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3"
    MODIFY COLUMN "amount" SET MASKING POLICY mask_financial_amount;
```

### Verify Policy Enforcement

```sql
SELECT *
FROM TABLE(INFORMATION_SCHEMA.POLICY_REFERENCES(
    REF_ENTITY_NAME => 'glue_iceberg_db."your_glue_database"."customer_orders_v3"',
    REF_ENTITY_DOMAIN => 'TABLE'
));
```

| POLICY_NAME | POLICY_KIND | REF_COLUMN_NAME | POLICY_STATUS |
|------------|-------------|-----------------|---------------|
| MASK_FINANCIAL_AMOUNT | MASKING_POLICY | amount | **ACTIVE** |

### Object Tagging (Horizon)

```sql
ALTER ICEBERG TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3"
    SET TAG SNOWFLAKE.CORE.SEMANTIC_CATEGORY = 'FINANCIAL';

SELECT SYSTEM$GET_TAG(
    'SNOWFLAKE.CORE.SEMANTIC_CATEGORY',
    'glue_iceberg_db."your_glue_database"."customer_orders_v3"',
    'TABLE'
);
-- Returns: FINANCIAL
```

### Row Access Policies

```sql
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
```

> All governance policies survive schema evolution via CLD. If EMR adds a column, your masking and row access policies remain enforced on existing columns.

---

## 8. Dynamic Iceberg Table (Pipeline Automation)

```sql
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
```

> **Note:** Dynamic Iceberg Table output must be Snowflake-managed (`CATALOG = 'SNOWFLAKE'`). The *source* can be externally managed via CLD.

---

## 9. Consuming the Stream (Merge Pattern)

```sql
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
```

---

## 10. CLD Schema Sync Verification

After running `ALTER TABLE ... ADD COLUMN loyalty_tier STRING` in EMR:

```sql
DESCRIBE TABLE glue_iceberg_db."your_glue_database"."customer_orders_v3";

SELECT "order_id", "product_name", "loyalty_tier"
FROM glue_iceberg_db."your_glue_database"."customer_orders_v3";
```

> CLD auto-syncs schema changes when the next snapshot arrives. Existing rows show NULL for new columns; new rows will have data. Schema-only changes (no DML) require a data commit to trigger sync.

---

## Test Results Summary (March 23, 2026)

| Test | Result |
|------|--------|
| Standard stream on externally managed V3 (via CLD) | **PASS** — mode: DEFAULT |
| INSERT captured in stream | **PASS** — `METADATA$ACTION = INSERT` |
| UPDATE captured as DELETE + INSERT pair | **PASS** — `METADATA$ISUPDATE = true`, same ROW_ID |
| DELETE captured in stream | **PASS** — `METADATA$ACTION = DELETE` |
| `METADATA$ROW_ID` present | **PASS** — V3 row lineage IDs tracked |
| Deletion vectors (`.puffin` files) present | **PASS** — merge-on-read confirmed |
| Masking policy on Iceberg table | **PASS** — ACTIVE enforcement |
| Object tagging (Horizon) | **PASS** — `SEMANTIC_CATEGORY = FINANCIAL` |
| Row access policy on Iceberg table | **PASS** — role-based row filtering |
| CLD schema sync (ADD/DROP/RENAME column) | **PASS** — auto-detected on next snapshot |
| Dynamic Iceberg Table from CLD source | **PASS** — Snowflake-managed output in Iceberg format |

### Engine Row Lineage Support

| Engine | V3 Format | Row Lineage | Full CDC |
|--------|-----------|-------------|----------|
| AWS Glue 5.0 | Yes | **No** | No |
| EMR 7.12 (Spark 3.5.6) | Yes | **Yes** | **Yes** |
| Snowflake (managed) | Yes | **Yes** | **Yes** |

---

## Important Notes

1. **Iceberg V3 support is in Public Preview** as of March 2026 — available to all Snowflake accounts.
2. The external engine **must** write proper row lineage: `_row_id = NULL` for new rows, maintain `_row_id` on copy-on-write.
3. `MAX_DATA_EXTENSION_TIME_IN_DAYS` does not work on externally managed V3 tables.
4. Dynamic Iceberg Table output can only be Snowflake-managed, not externally managed.
5. Column names from Glue/Spark are **case-sensitive** in Snowflake — use double quotes (e.g., `"order_id"`).
6. Governance policies (masking, row access, tagging) persist across CLD schema evolution.

---

## License

MIT — use freely, attribution appreciated.
