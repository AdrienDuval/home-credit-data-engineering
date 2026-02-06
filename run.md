# Run guide — Bronze ingestion

Commands to run from your host (PowerShell or bash). Containers must be up (`docker compose up -d`).

**Log files:** Bronze: `logs/bronze/feeder_postgres.txt`, `logs/bronze/feeder_csv.txt`. Silver: `logs/silver/processor.txt`. Gold: `logs/gold/processor.txt`. All append; use for debugging.

---

## 1. One-time HDFS setup

Run **once** (or when you see *Permission denied* writing to HDFS). Creates `/raw`, `/silver`, and `/gold` and allows Spark to write.

```bash
docker exec -it home_credit_namenode hdfs dfs -mkdir -p /raw /silver /gold
docker exec -it home_credit_namenode hdfs dfs -chmod -R 777 /raw /silver /gold
```

---

## 2. Bronze — PostgreSQL (Source A)

Ingests `raw.application_train` and `raw.application_test` from Postgres to HDFS.  
Default table: `application_train`. Override with `--table-name application_test` if needed.

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/work-dir/jars/postgresql-42.7.9.jar /opt/spark/work-dir/spark/bronze/feeder_postgres.py
```

**Second table (application_test):**

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/work-dir/jars/postgresql-42.7.9.jar /opt/spark/work-dir/spark/bronze/feeder_postgres.py --table-name application_test
```

---

## 3. Bronze — CSV (Source B)

One command per CSV file. Output path: `hdfs://namenode:8020/raw/csv/<dataset_name>/ingest_date=YYYY-MM-DD/` (same partition style as Postgres).

| Dataset              | Command |
|----------------------|--------|
| **bureau**           | Below  |
| bureau_balance       | ↓      |
| installments_payments | ↓      |
| credit_card_balance   | ↓      |
| pos_cash_balance      | ↓      |
| previous_application | ↓      |

**bureau**

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/bureau.csv --dataset-name bureau
```

**bureau_balance**

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/bureau_balance.csv --dataset-name bureau_balance
```

**installments_payments**

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/installments_payments.csv --dataset-name installments_payments
```

**credit_card_balance**

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/credit_card_balance.csv --dataset-name credit_card_balance
```

**pos_cash_balance** (file: `POS_CASH_balance.csv`)

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/POS_CASH_balance.csv --dataset-name pos_cash_balance
```

**previous_application**

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/previous_application.csv --dataset-name previous_application
```

---

## 4. Silver — Step 1: silver_client_application

**Prerequisite:** Bronze Postgres ingestion must be done (Section 2: both `application_train` and `application_test`).

Builds the first Silver table from Bronze Postgres: validates rows (7 rules), drops invalid, writes to `hdfs://namenode:8020/silver/silver_client_application/year=.../month=.../day=...`. Check `logs/silver/processor.txt` for validation counts and any errors.

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/silver/processor.py
```

With custom date (optional):

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/silver/processor.py --ingest-date 2026-02-05
```

**What to check after running:** Logs show Bronze row counts, validation failure counts per rule, and final valid row count. On HDFS (e.g. http://localhost:9870) browse `/silver/silver_client_application/` and confirm partition folders `year=.../month=.../day=...` and Parquet files.

### Silver — Step 2: silver_bureau_summary

**Prerequisite:** Bronze CSV ingestion must be done for `bureau` and `bureau_balance` (Section 3).

This step is also run by the same `processor.py` command above (runs Steps 1–4). It:

- Reads Bronze Parquet from:
  - `hdfs://namenode:8020/raw/csv/bureau/ingest_date=...`
  - `hdfs://namenode:8020/raw/csv/bureau_balance/ingest_date=...`
- Uses a **window function** over `SK_ID_BUREAU`, ordered by `MONTHS_BALANCE desc`, to keep **only the latest monthly status** per bureau credit.
- Joins that latest status back to `bureau` and aggregates to **client level (`SK_ID_CURR`)**:
  - `bureau_credit_count` – total number of bureau credits
  - `bureau_active_credit_count` – how many are currently active
  - `bureau_total_debt` – sum of `AMT_CREDIT_SUM_DEBT`
  - `bureau_max_days_overdue` – max `CREDIT_DAY_OVERDUE`
  - `bureau_total_overdue` – sum of `AMT_CREDIT_SUM_OVERDUE`
- Writes to `hdfs://namenode:8020/silver/silver_bureau_summary/year=.../month=.../day=...`.

**What to check:** In `logs/silver/processor.txt`, look for the `silver_bureau_summary` row counts and then browse `/silver/silver_bureau_summary/` in HDFS to see the partition for your `ingest-date`.

### Silver — Step 3: silver_payment_behavior

**Prerequisite:** Bronze CSV ingestion must be done for `installments_payments` (Section 3).

This step is also run by the same `processor.py` command above. It:

- Reads Bronze Parquet from:
  - `hdfs://namenode:8020/raw/csv/installments_payments/ingest_date=...`
- Computes **delay** = `DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT` (positive = late, negative = early).
- Uses a **window function** per client: `partition by SK_ID_CURR order by DAYS_ENTRY_PAYMENT` (client payment timeline).
- Aggregates to **client level (`SK_ID_CURR`)**:
  - `payment_avg_delay_days` – average delay (positive = often late)
  - `payment_late_count` – number of installments paid late
  - `payment_total_paid` – sum of amounts paid
  - `payment_total_installment` – sum of amounts due
  - `payment_ratio` – total_paid / total_installment
- Caches the installments read (visible in Spark UI storage).
- Writes to `hdfs://namenode:8020/silver/silver_payment_behavior/year=.../month=.../day=...`.

**What to check:** In `logs/silver/processor.txt`, look for `silver_payment_behavior` row counts and browse `/silver/silver_payment_behavior/` in HDFS.

### Silver — Step 4: silver_previous_applications

**Prerequisite:** Bronze CSV ingestion must be done for `previous_application` (Section 3).

This step is also run by the same `processor.py` command above. It:

- Reads Bronze Parquet from:
  - `hdfs://namenode:8020/raw/csv/previous_application/ingest_date=...`
- Aggregates to **client level (`SK_ID_CURR`)**:
  - `previous_app_count` – total number of previous applications
  - `previous_rejection_rate` – percentage that were rejected
  - `previous_avg_requested` – average amount requested
  - `previous_avg_granted` – average amount granted
  - Status distribution counts (how many of each status type)
- Writes to `hdfs://namenode:8020/silver/silver_previous_applications/year=.../month=.../day=...`.

**What to check:** In `logs/silver/processor.txt`, look for `silver_previous_applications` row counts and browse `/silver/silver_previous_applications/` in HDFS.

---

## 5. Gold — client risk profile & portfolio risk

**Prerequisite:** Silver processor must have been run (Section 4) for the same `--ingest-date` you use below. All four Silver tables must exist for that partition.

Builds two Gold tables from Silver:

1. **gold_client_risk_profile** — 1 row per client (`SK_ID_CURR`): joins `silver_client_application`, `silver_bureau_summary`, `silver_payment_behavior`, `silver_previous_applications`. Adds: `income`, `credit_exposure`, `bureau_debt_ratio`, `payment_delay_score`, `previous_rejection_rate`, `default_flag` (TARGET), `risk_segment` (HIGH / MEDIUM / LOW).
2. **gold_portfolio_risk** — 1 row per `risk_segment`: `client_count`, `total_exposure`, `avg_default_rate`, `avg_income`.

**Use the same ingest-date as your Silver run** (e.g. the partition that exists under `/silver/.../year=.../month=.../day=...`).

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/gold/processor.py
```

With custom date (must match Silver partition):

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/gold/processor.py --ingest-date 2026-02-06
```

**What to check:** In `logs/gold/processor.txt`, look for row counts for both Gold tables. On HDFS browse `/gold/gold_client_risk_profile/` and `/gold/gold_portfolio_risk/` for the partition.

---

## 6. Preview Bronze & Silver data

### Method 1: Using preview script (recommended)

Preview any Bronze or Silver Parquet table. Shows row count, schema, and sample rows.

**Bronze examples:**

```bash
# Preview Bronze CSV bureau
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/preview_data.py --path hdfs://namenode:8020/raw/csv/bureau --rows 10

# Preview Bronze Postgres application_train
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/preview_data.py --path hdfs://namenode:8020/raw/postgres/application/application_train --rows 10
```

**Silver examples:**

```bash
# Preview Silver client_application (replace year/month/day with your actual partition)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/preview_data.py --path hdfs://namenode:8020/silver/silver_client_application/year=2026/month=2/day=6 --rows 10

# Preview Silver bureau_summary
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/preview_data.py --path hdfs://namenode:8020/silver/silver_bureau_summary/year=2026/month=2/day=6 --rows 10
```

### Method 2: Using pyspark shell (interactive)

Start an interactive Spark shell and query data directly:

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/pyspark --master spark://spark-master:7077
```

Then in the shell:

```python
# Read Bronze CSV bureau
df = spark.read.parquet("hdfs://namenode:8020/raw/csv/bureau")
df.show(10)
df.printSchema()
df.count()

# Read Silver client_application
df_silver = spark.read.parquet("hdfs://namenode:8020/silver/silver_client_application/year=2026/month=2/day=6")
df_silver.show(10)
df_silver.select("SK_ID_CURR", "AMT_INCOME_TOTAL", "AMT_CREDIT", "CODE_GENDER").show(20)
```

### Method 3: HDFS web UI (browse files only)

Open http://localhost:9870 in your browser, then:

- Navigate to `/raw/csv/bureau/ingest_date=2026-02-06/` to see Parquet files
- Navigate to `/silver/silver_client_application/year=2026/month=2/day=6/` to see Silver files

**Note:** The web UI shows file metadata (size, owner, etc.) but **not** the data contents. Use Method 1 or 2 to see actual rows.

---

## 7. Export samples to Excel (for visualization)

Export sample rows from Bronze or Silver Parquet tables to Excel (.xlsx) for analysis in Excel/Power BI.

**Prerequisites:** Install Python packages in the Spark container (run once):

```bash
# Install as root (required due to Spark container permissions)
docker exec -it -u root home_credit_spark_master pip install pandas openpyxl
```

**Note:** The Spark user doesn't have write permissions to `/home/spark`, so root installation is required. This installs packages system-wide in the container.

**Note:** If `openpyxl` is not available, the script will fall back to CSV export.

**Bronze exports:**

```bash
# Export Bronze CSV bureau sample (1000 rows)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/export_to_excel.py --hdfs-path hdfs://namenode:8020/raw/csv/bureau --output /opt/spark/work-dir/exports/bronze_bureau_sample.xlsx --rows 1000

# Export Bronze Postgres application_train sample
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/export_to_excel.py --hdfs-path hdfs://namenode:8020/raw/postgres/application/application_train --output /opt/spark/work-dir/exports/bronze_application_train_sample.xlsx --rows 1000
```

**Silver exports:**

```bash
# Export Silver client_application sample (replace year/month/day with your partition)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/export_to_excel.py --hdfs-path hdfs://namenode:8020/silver/silver_client_application/year=2026/month=2/day=6 --output /opt/spark/work-dir/exports/silver_client_application_sample.xlsx --rows 1000

# Export Silver bureau_summary sample
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/export_to_excel.py --hdfs-path hdfs://namenode:8020/silver/silver_bureau_summary/year=2026/month=2/day=6 --output /opt/spark/work-dir/exports/silver_bureau_summary_sample.xlsx --rows 1000

# Export Silver payment_behavior sample
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/export_to_excel.py --hdfs-path hdfs://namenode:8020/silver/silver_payment_behavior/year=2026/month=2/day=6 --output /opt/spark/work-dir/exports/silver_payment_behavior_sample.xlsx --rows 1000

# Export Silver previous_applications sample
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/export_to_excel.py --hdfs-path hdfs://namenode:8020/silver/silver_previous_applications/year=2026/month=2/day=6 --output /opt/spark/work-dir/exports/silver_previous_applications_sample.xlsx --rows 1000
```

**Gold exports:**

```bash
# Export Gold client_risk_profile sample (replace year/month/day with your partition)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/export_to_excel.py --hdfs-path hdfs://namenode:8020/gold/gold_client_risk_profile/year=2026/month=2/day=6 --output /opt/spark/work-dir/exports/gold_client_risk_profile_sample.xlsx --rows 1000

# Export Gold portfolio_risk (small table: one row per segment)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/common/export_to_excel.py --hdfs-path hdfs://namenode:8020/gold/gold_portfolio_risk/year=2026/month=2/day=6 --output /opt/spark/work-dir/exports/gold_portfolio_risk_sample.xlsx --rows 100
```

**After export:** Files are written to `/opt/spark/work-dir/exports/` inside the container, which maps to `./exports/` on your host. Copy them out:

```bash
docker cp home_credit_spark_master:/opt/spark/work-dir/exports/. ./exports/
```

Or access directly if the `exports/` folder is mounted in docker-compose.

---

## 8. Test logger (intentional failure)

Use this to confirm that errors are written to the log file. The command below **will fail** (non-existent CSV path). After it fails, check `logs/bronze/feeder_csv.txt` — you should see an `ERROR` line and a full traceback.

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/does_not_exist.csv --dataset-name bureau
```

**Expected:** Non-zero exit code and stack trace in the terminal; the same error and traceback in `logs/bronze/feeder_csv.txt`.

---

## 9. Concepts (reference)

- **Lazy evaluation:** `df.read` and `df.write` build a plan; execution happens on actions (e.g. `count()`, `write`).
- **Partitioning:** Data is split into partitions for parallel processing.
- **Shuffle:** Data movement between stages (e.g. aggregations).
- **Data locality:** Tasks run where the data is (e.g. `PROCESS_LOCAL`).
- **Bronze layer:** Raw, unprocessed data ingested from sources (PostgreSQL / CSV → HDFS Parquet).
- **Why multiple `part-*.parquet` files?** Spark writes one file per *task partition* of the DataFrame. So 8 partitions → 8 part files. This is normal: it allows parallel writes and reads. The number of parts is the number of Spark partitions (e.g. from default parallelism or from the data size). You can coalesce to fewer files if you need (e.g. `.coalesce(1)`) at the cost of less parallelism.
