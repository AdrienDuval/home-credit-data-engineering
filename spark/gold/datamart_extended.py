"""
Extended datamart loader: write bureau credits and previous applications to PostgreSQL.
Reads from Bronze (CSV-ingested Parquet), writes to:
  - datamart.datamart_client_bureau (one row per bureau credit per client)
  - datamart.datamart_client_previous_apps (one row per previous application per client)

Run after Bronze ingestion (same ingest_date). Enables API endpoints for per-client
bureau credits and previous applications.

Usage:
  spark-submit --jars /path/to/postgresql.jar spark/gold/datamart_extended.py \\
    --bronze-csv-base hdfs://namenode:8020/raw/csv \\
    --ingest-date 2026-02-06 \\
    --jdbc-url jdbc:postgresql://postgres:5432/home_credit \\
    --jdbc-user home_credit_user --jdbc-password ...
"""
import argparse
import datetime
import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_SCRIPT_DIR))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from spark.common.logger import setup_logger  # noqa: E402

logger = setup_logger(
    __name__,
    log_dir="logs/gold",
    log_file_basename="datamart_extended",
)

DEFAULT_BRONZE_CSV_BASE = os.environ.get("BRONZE_CSV_BASE", "hdfs://namenode:8020/raw/csv")
DEFAULT_INGEST_DATE = datetime.date.today().isoformat()


def partition_path_for_ingest_date(bronze_base: str, dataset: str, ingest_date_str: str) -> str:
    """Bronze CSV feeder writes partition ingest_date=YYYY-MM-DD (same as feeder_csv.py)."""
    return f"{bronze_base.rstrip('/')}/{dataset}/ingest_date={ingest_date_str}"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load Bronze bureau and previous_application into PostgreSQL datamart"
    )
    parser.add_argument(
        "--bronze-csv-base",
        default=DEFAULT_BRONZE_CSV_BASE,
        help="Base HDFS path for Bronze CSV data (e.g. hdfs://namenode:8020/raw/csv)",
    )
    parser.add_argument(
        "--ingest-date",
        default=DEFAULT_INGEST_DATE,
        help="Partition date YYYY-MM-DD (must match Bronze run)",
    )
    parser.add_argument(
        "--jdbc-url",
        default=os.environ.get("GOLD_JDBC_URL", "jdbc:postgresql://postgres:5432/home_credit"),
        help="JDBC URL for PostgreSQL",
    )
    parser.add_argument(
        "--jdbc-user",
        default=os.environ.get("POSTGRES_USER", "home_credit_user"),
        help="PostgreSQL user",
    )
    parser.add_argument(
        "--jdbc-password",
        default=os.environ.get("POSTGRES_PASSWORD", "home_credit_pwd"),
        help="PostgreSQL password",
    )
    return parser.parse_args()


def _select_bureau_columns(df):
    """Select and alias bureau columns for datamart (lowercase for PostgreSQL)."""
    mapping = []
    for col_name in df.columns:
        c = col_name.strip()
        if not c:
            continue
        low = c.lower()
        mapping.append(F.col(c).alias(low))
    if not mapping:
        return df
    return df.select(mapping)


def _select_previous_columns(df):
    """Select and alias previous_application columns for datamart (lowercase)."""
    mapping = []
    for col_name in df.columns:
        c = col_name.strip()
        if not c:
            continue
        low = c.lower()
        mapping.append(F.col(c).alias(low))
    if not mapping:
        return df
    return df.select(mapping)


def main():
    args = parse_args()
    logger.info(
        "Datamart extended start: bronze_csv_base=%s, ingest_date=%s",
        args.bronze_csv_base,
        args.ingest_date,
    )

    try:
        spark = (
            SparkSession.builder
            .appName("gold_datamart_extended")
            .getOrCreate()
        )

        bronze_base = args.bronze_csv_base.rstrip("/")
        jdbc_options = {
            "url": args.jdbc_url,
            "user": args.jdbc_user,
            "password": args.jdbc_password,
            "driver": "org.postgresql.Driver",
        }

        # --- Bureau: one row per credit per client (Bronze path: .../bureau/ingest_date=YYYY-MM-DD) ---
        path_bureau = partition_path_for_ingest_date(bronze_base, "bureau", args.ingest_date)
        logger.info("Reading Bronze bureau from %s", path_bureau)
        try:
            df_bureau = spark.read.parquet(path_bureau)
            df_bureau = _select_bureau_columns(df_bureau)
            # Drop partition/metadata columns if present
            for c in ["year", "month", "day", "ingest_date", "source_system"]:
                if c in df_bureau.columns:
                    df_bureau = df_bureau.drop(c)
            n_bureau = df_bureau.count()
            logger.info("Bureau rows to write: %s", n_bureau)
            table_bureau = "datamart.datamart_client_bureau"
            df_bureau.write.format("jdbc").mode("overwrite").options(
                **jdbc_options, dbtable=table_bureau
            ).save()
            logger.info("Written to %s", table_bureau)
        except Exception as e:
            logger.warning("Bureau load skipped (path or schema): %s", e)

        # --- Previous applications: one row per previous app per client ---
        path_prev = partition_path_for_ingest_date(bronze_base, "previous_application", args.ingest_date)
        logger.info("Reading Bronze previous_application from %s", path_prev)
        try:
            df_prev = spark.read.parquet(path_prev)
            df_prev = _select_previous_columns(df_prev)
            for c in ["year", "month", "day", "ingest_date", "source_system"]:
                if c in df_prev.columns:
                    df_prev = df_prev.drop(c)
            n_prev = df_prev.count()
            logger.info("Previous application rows to write: %s", n_prev)
            table_prev = "datamart.datamart_client_previous_apps"
            df_prev.write.format("jdbc").mode("overwrite").options(
                **jdbc_options, dbtable=table_prev
            ).save()
            logger.info("Written to %s", table_prev)
        except Exception as e:
            logger.warning("Previous applications load skipped: %s", e)

        spark.stop()
        logger.info("Datamart extended load completed")
    except Exception:
        logger.exception("Datamart extended load failed")
        raise


if __name__ == "__main__":
    main()
