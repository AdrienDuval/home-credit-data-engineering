# Run guide — Bronze ingestion

Commands to run from your host (PowerShell or bash). Containers must be up (`docker compose up -d`).

**Log files:** Feeder jobs write to `logs/bronze/feeder_postgres.txt` and `logs/bronze/feeder_csv.txt` (append). Use these to debug failures when something goes wrong.

---

## 1. One-time HDFS setup

Run **once** (or when you see *Permission denied* writing to HDFS). Creates `/raw` and allows Spark to write.

```bash
docker exec -it home_credit_namenode hdfs dfs -mkdir -p /raw
docker exec -it home_credit_namenode hdfs dfs -chmod -R 777 /raw
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

One command per CSV file. Run each line to ingest that dataset into `hdfs://namenode:8020/raw/csv/<dataset_name>/`.

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

## 4. Test logger (intentional failure)

Use this to confirm that errors are written to the log file. The command below **will fail** (non-existent CSV path). After it fails, check `logs/bronze/feeder_csv.txt` — you should see an `ERROR` line and a full traceback.

```bash
docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 /opt/spark/work-dir/spark/bronze/feeder_csv.py --input-path /opt/spark/work-dir/data/does_not_exist.csv --dataset-name bureau
```

**Expected:** Non-zero exit code and stack trace in the terminal; the same error and traceback in `logs/bronze/feeder_csv.txt`.

---

## 5. Concepts (reference)

- **Lazy evaluation:** `df.read` and `df.write` build a plan; execution happens on actions (e.g. `count()`, `write`).
- **Partitioning:** Data is split into partitions for parallel processing.
- **Shuffle:** Data movement between stages (e.g. aggregations).
- **Data locality:** Tasks run where the data is (e.g. `PROCESS_LOCAL`).
- **Bronze layer:** Raw, unprocessed data ingested from sources (PostgreSQL / CSV → HDFS Parquet).
