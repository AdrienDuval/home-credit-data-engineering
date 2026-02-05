"""
Bronze ingestion from CSV files (Source B).
Reads a single CSV dataset, adds lineage columns, writes to HDFS as Parquet
partitioned by ingestion date (year/month/day). No business transformation.
"""
import argparse
import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Project root on path so we can import spark.common when run via spark-submit
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_SCRIPT_DIR))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from spark.common.logger import setup_logger  # noqa: E402

logger = setup_logger(
    __name__,
    log_dir="logs/bronze",
    log_file_basename="feeder_csv",
)

# Defaults (overridable by env or CLI)
DEFAULTS = {
    "input_path": os.environ.get("CSV_INPUT_PATH", "/opt/spark/work-dir/data"),
    "dataset_name": os.environ.get("CSV_DATASET_NAME", "bureau"),
    "hdfs_base_path": os.environ.get("HDFS_CSV_BASE_PATH", "hdfs://namenode:8020/raw/csv"),
    "ingest_date": datetime.date.today().isoformat(),
}


def parse_args():
    parser = argparse.ArgumentParser(
        description="Bronze ingestion from CSV: read one dataset, write to HDFS (Parquet, date-partitioned)."
    )
    parser.add_argument(
        "--input-path",
        default=DEFAULTS["input_path"],
        help="Path to CSV file or folder (e.g. /data/csv/bureau.csv or /data/csv/bureau)",
    )
    parser.add_argument(
        "--dataset-name",
        default=DEFAULTS["dataset_name"],
        help="Dataset name for HDFS path (e.g. bureau, bureau_balance, installments_payments)",
    )
    parser.add_argument(
        "--hdfs-base-path",
        default=DEFAULTS["hdfs_base_path"],
        help="Base HDFS path for raw CSV (e.g. hdfs://namenode:8020/raw/csv)",
    )
    parser.add_argument(
        "--ingest-date",
        default=DEFAULTS["ingest_date"],
        help="Ingestion date YYYY-MM-DD (used for partitioning)",
    )
    return parser.parse_args()


def parse_ingest_date(ingest_date_str: str):
    """Parse YYYY-MM-DD into year, month, day for partition columns."""
    dt = datetime.datetime.strptime(ingest_date_str, "%Y-%m-%d")
    return dt.year, dt.month, dt.day


def main():
    args = parse_args()
    logger.info("Starting Bronze CSV ingestion job")
    logger.info("Dataset: %s", args.dataset_name)
    logger.info("Input path: %s", args.input_path)
    logger.info("HDFS base path: %s", args.hdfs_base_path)
    logger.info("Ingest date: %s", args.ingest_date)

    try:
        year, month, day = parse_ingest_date(args.ingest_date)
    except ValueError as e:
        logger.error("Invalid ingest-date (use YYYY-MM-DD): %s", e)
        raise

    try:
        # -------------------------------------------------------------------------
        # Spark session
        # -------------------------------------------------------------------------
        spark = (
            SparkSession.builder.appName(f"bronze_csv_{args.dataset_name}")
            .getOrCreate()
        )

        # -------------------------------------------------------------------------
        # Read CSV (raw: header + schema inference, no business logic)
        # -------------------------------------------------------------------------
        logger.info("Reading CSV from %s", args.input_path)
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(args.input_path)
        )

        source_count = df.count()
        logger.info("Rows read from source: %s", source_count)

        # -------------------------------------------------------------------------
        # Add lineage columns (allowed in Bronze)
        # -------------------------------------------------------------------------
        df_bronze = (
            df.withColumn("ingest_date", lit(args.ingest_date))
            .withColumn("source_system", lit("csv"))
            .withColumn("year", lit(year))
            .withColumn("month", lit(month))
            .withColumn("day", lit(day))
        )

        # -------------------------------------------------------------------------
        # Write to HDFS: Parquet, partitioned by year/month/day (exam requirement)
        # -------------------------------------------------------------------------
        output_path = f"{args.hdfs_base_path.rstrip('/')}/{args.dataset_name}"
        logger.info("Writing Bronze data to HDFS: %s (partitioned by year/month/day)", output_path)

        (
            df_bronze.write
            .mode("overwrite")
            .partitionBy("year", "month", "day")
            .parquet(output_path)
        )

        logger.info("Rows written to Bronze: %s", source_count)
        logger.info("Bronze CSV ingestion completed successfully")
        spark.stop()
    except Exception:
        logger.exception("Bronze CSV ingestion failed")
        raise


if __name__ == "__main__":
    main()
