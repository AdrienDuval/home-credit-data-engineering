# Full datamart setup — step-by-step

This checklist ensures the PostgreSQL datamart has **all** data needed for the dashboard and client detail page:

- **datamart_client_risk** — client risk profile (list + detail by ID)
- **datamart_portfolio_summary** — portfolio charts
- **datamart_client_bureau** — bureau credits per client (client detail page)
- **datamart_client_previous_apps** — previous applications per client (client detail page)

Use **one ingest date** for the whole run (e.g. `2026-02-07`). Replace `2026-02-07` in the commands below with your date.

---

## Prerequisites

- Docker Compose services running: `docker compose up -d`
- PostgreSQL has schema `raw` and tables `raw.application_train`, `raw.application_test` with data
- CSV files in `data/` (at least: `bureau.csv`, `bureau_balance.csv`, `installments_payments.csv`, `previous_application.csv`)

---

## Step 1 — HDFS directories (one-time)

```bash
docker exec -it home_credit_namenode hdfs dfs -mkdir -p /raw /silver /gold
docker exec -it home_credit_namenode hdfs dfs -chmod -R 777 /raw /silver /gold
```

---

## Step 2 — Bronze: PostgreSQL tables

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/work-dir/jars/postgresql-42.7.9.jar /opt/spark/work-dir/spark/bronze/feeder_postgres.py --ingest-date 2026-02-07

docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/work-dir/jars/postgresql-42.7.9.jar /opt/spark/work-dir/spark/bronze/feeder_postgres.py --table-name application_test --ingest-date 2026-02-07
```

---

## Step 3 — Bronze: CSV files (required for bureau & previous apps)

At least **bureau**, **bureau_balance**, **previous_application** (and for Silver/Gold: **installments_payments**). Use the **same** `--ingest-date 2026-02-07`.

```bash
# Bureau (for bureau summary Silver + extended datamart)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/bureau.csv --dataset-name bureau --ingest-date 2026-02-07

# Bureau balance (for Silver bureau summary)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/bureau_balance.csv --dataset-name bureau_balance --ingest-date 2026-02-07

# Previous applications (for extended datamart + Silver)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/previous_application.csv --dataset-name previous_application --ingest-date 2026-02-07

# Installments payments (for Silver payment behavior)
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/installments_payments.csv --dataset-name installments_payments --ingest-date 2026-02-07
```

---

## Step 4 — Silver processor

Builds Silver tables (client_application, bureau_summary, payment_behavior, previous_applications). Uses the same partition date.

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/silver/processor.py --ingest-date 2026-02-07
```

Check `logs/silver/processor.txt` for row counts and errors.

---

## Step 5 — Gold processor

Builds Gold tables on HDFS (gold_client_risk_profile, gold_portfolio_risk).

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/gold/processor.py --ingest-date 2026-02-07
```

Check `logs/gold/processor.txt`.

---

## Step 6 — Main datamart (client risk + portfolio summary)

Loads **datamart_client_risk** and **datamart_portfolio_summary** from Gold into PostgreSQL. This fixes the client list and charts, and enables **client detail by ID** (risk profile).

**One-time:** create schema (if not exists):

```bash
docker exec -it home_credit_postgres psql -U home_credit_user -d home_credit -c "CREATE SCHEMA IF NOT EXISTS datamart;"
```

**Run Gold with write-datamart:**

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/work-dir/jars/postgresql-42.7.9.jar /opt/spark/work-dir/spark/gold/processor.py --ingest-date 2026-02-07 --write-datamart --jdbc-url "jdbc:postgresql://postgres:5432/home_credit" --jdbc-user home_credit_user --jdbc-password home_credit_pwd
```

After this:

- `GET /clients/risk` and pagination work
- `GET /clients/risk/{id}` returns the client risk profile (no more 404 for valid IDs from the list)
- `GET /portfolio/summary` returns data for the three charts

---

## Step 7 — Extended datamart (bureau + previous applications)

Loads **datamart_client_bureau** and **datamart_client_previous_apps** from Bronze CSV into PostgreSQL. Reads from the same paths Bronze writes: `/raw/csv/bureau/ingest_date=YYYY-MM-DD` and `/raw/csv/previous_application/ingest_date=YYYY-MM-DD`. Use the **same** `--ingest-date 2026-02-07` as in **Step 3** (Bronze CSV).

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 --jars /opt/spark/work-dir/jars/postgresql-42.7.9.jar /opt/spark/work-dir/spark/gold/datamart_extended.py --ingest-date 2026-02-07 --bronze-csv-base hdfs://namenode:8020/raw/csv --jdbc-url "jdbc:postgresql://postgres:5432/home_credit" --jdbc-user home_credit_user --jdbc-password home_credit_pwd
```

After this:

- `GET /clients/risk/{id}/bureau` returns bureau credits for that client
- `GET /clients/risk/{id}/previous-applications` returns previous applications for that client
- The client detail page shows the two tables instead of “No data”

---

## Step 8 — Verify in PostgreSQL

```bash
# Tables in datamart
docker exec -it home_credit_postgres psql -U home_credit_user -d home_credit -c "\dt datamart.*"

# Row counts
docker exec -it home_credit_postgres psql -U home_credit_user -d home_credit -c "SELECT 'datamart_client_risk' AS tbl, COUNT(*) FROM datamart.datamart_client_risk UNION ALL SELECT 'datamart_portfolio_summary', COUNT(*) FROM datamart.datamart_portfolio_summary UNION ALL SELECT 'datamart_client_bureau', COUNT(*) FROM datamart.datamart_client_bureau UNION ALL SELECT 'datamart_client_previous_apps', COUNT(*) FROM datamart.datamart_client_previous_apps;"

# Sample client (replace 100002 with an ID that appears in your list)
docker exec -it home_credit_postgres psql -U home_credit_user -d home_credit -c "SELECT * FROM datamart.datamart_client_risk LIMIT 1;"
```

Expected:

- **datamart_client_risk**: one row per client (e.g. hundreds of thousands)
- **datamart_portfolio_summary**: 3 rows (HIGH, MEDIUM, LOW)
- **datamart_client_bureau**: one row per bureau credit (many rows)
- **datamart_client_previous_apps**: one row per previous application (many rows)

---

## Quick reference — minimal command order

| Step | What it fills | Command (replace 2026-02-07) |
|------|----------------|------------------------------|
| 1 | HDFS dirs | `docker exec ... hdfs dfs -mkdir -p /raw /silver /gold` + chmod |
| 2 | Bronze Postgres | `feeder_postgres.py` for application_train + application_test with `--ingest-date 2026-02-07` |
| 3 | Bronze CSV | `feeder_csv.py` for bureau, bureau_balance, previous_application, installments_payments with `--ingest-date 2026-02-07` |
| 4 | Silver | `spark/silver/processor.py --ingest-date 2026-02-07` |
| 5 | Gold | `spark/gold/processor.py --ingest-date 2026-02-07` |
| 6 | Main datamart | Same as step 5 with `--write-datamart` (or run processor again with `--write-datamart`) |
| 7 | Extended datamart | `spark/gold/datamart_extended.py --ingest-date 2026-02-07 ...` |

If **client detail by ID** still returns 404 after step 6, the table might use a different column name for the client ID. Run:

```bash
docker exec -it home_credit_postgres psql -U home_credit_user -d home_credit -c "\d datamart.datamart_client_risk"
```

and check the exact name of the first column (e.g. `sk_id_curr` vs `SK_ID_CURR`). The API tries both.
