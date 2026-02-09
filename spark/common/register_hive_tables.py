"""
Register Silver and Gold Parquet tables (on HDFS) as Hive tables in the Hive Metastore.
Run after Silver and Gold processors have written data. Enables querying via HiveQL (Beeline)
or Spark SQL using table names instead of paths.

Usage (from project root, with Hive Metastore and HDFS up):
  docker exec -it home_credit_spark_master /opt/spark/bin/spark-submit --master spark://spark-master:7077 \\
    /opt/spark/work-dir/spark/common/register_hive_tables.py \\
    --metastore-uri thrift://hive-metastore:9083 \\
    --ingest-date 2026-02-07
"""
import argparse
import datetime
import os
import sys

from pyspark.sql import SparkSession

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(_SCRIPT_DIR)
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from spark.common.logger import setup_logger  # noqa: E402

logger = setup_logger(
    __name__,
    log_dir="logs/hive",
    log_file_basename="register_hive_tables",
)

DEFAULT_METASTORE_URI = os.environ.get("HIVE_METASTORE_URIS", "thrift://hive-metastore:9083")
DEFAULT_HDFS_NAMENODE = os.environ.get("HDFS_NAMENODE", "hdfs://namenode:8020")
DEFAULT_INGEST_DATE = datetime.date.today().isoformat()


def parse_args():
    p = argparse.ArgumentParser(description="Register Silver/Gold HDFS paths as Hive tables")
    p.add_argument("--metastore-uri", default=DEFAULT_METASTORE_URI, help="Hive Metastore thrift URI")
    p.add_argument("--hdfs-base", default=DEFAULT_HDFS_NAMENODE, help="HDFS namenode (e.g. hdfs://namenode:8020)")
    p.add_argument("--ingest-date", default=DEFAULT_INGEST_DATE, help="Partition date YYYY-MM-DD (for path hints)")
    return p.parse_args()


def main():
    args = parse_args()
    logger.info("Registering Hive tables: metastore_uri=%s, hdfs_base=%s", args.metastore_uri, args.hdfs_base)

    spark = (
        SparkSession.builder
        .appName("register_hive_tables")
        .config("spark.sql.warehouse.dir", f"{args.hdfs_base.rstrip('/')}/user/hive/warehouse")
        .config("hive.metastore.uris", args.metastore_uri)
        .enableHiveSupport()
        .getOrCreate()
    )

    base = args.hdfs_base.rstrip("/")

    # Databases
    for db in ("silver_db", "gold_db"):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")
        logger.info("Database %s ready", db)

    # Silver tables (external, point to existing Parquet)
    silver_tables = [
        ("silver_db", "silver_client_application", f"{base}/silver/silver_client_application"),
        ("silver_db", "silver_bureau_summary", f"{base}/silver/silver_bureau_summary"),
        ("silver_db", "silver_payment_behavior", f"{base}/silver/silver_payment_behavior"),
        ("silver_db", "silver_previous_applications", f"{base}/silver/silver_previous_applications"),
    ]
    for db, table, path in silver_tables:
        full_name = f"{db}.{table}"
        try:
            spark.catalog.createTable(full_name, path, source="parquet")
            logger.info("Registered Hive table: %s -> %s", full_name, path)
        except Exception as e:
            logger.warning("Skip %s: %s", full_name, e)

    # Gold tables
    gold_tables = [
        ("gold_db", "gold_client_risk_profile", f"{base}/gold/gold_client_risk_profile"),
        ("gold_db", "gold_portfolio_risk", f"{base}/gold/gold_portfolio_risk"),
    ]
    for db, table, path in gold_tables:
        full_name = f"{db}.{table}"
        try:
            spark.catalog.createTable(full_name, path, source="parquet")
            logger.info("Registered Hive table: %s -> %s", full_name, path)
        except Exception as e:
            logger.warning("Skip %s: %s", full_name, e)

    spark.stop()
    logger.info("Hive registration completed. Query via Beeline or Spark SQL: USE silver_db; SHOW TABLES;")


if __name__ == "__main__":
    main()
