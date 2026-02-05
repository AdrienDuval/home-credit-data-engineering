import argparse
import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Project root on path so we can import spark.common when run via spark-submit
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_SCRIPT_DIR))  # spark/bronze -> spark -> project root
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from spark.common.logger import setup_logger  # noqa: E402

logger = setup_logger(
    __name__,
    log_dir="logs/bronze",
    log_file_basename="feeder_postgres",
)

# Defaults
DEFAULTS = {
    "jdbc_url": os.environ.get("JDBC_URL", "jdbc:postgresql://postgres:5432/home_credit"),
    "db_user": os.environ.get("POSTGRES_USER", "home_credit_user"),
    "db_password": os.environ.get("POSTGRES_PASSWORD", "home_credit_pwd"),
    "db_schema": "raw",
    "table_name": "application_train",
    "hdfs_base_path": "hdfs://namenode:8020/raw/postgres",
    "ingest_date": datetime.date.today().isoformat(),
}

def parse_args():
    parser = argparse.ArgumentParser(description="Bronze ingestion from PostgreSQL")

    parser.add_argument("--jdbc-url", default=DEFAULTS["jdbc_url"], help="PostgreSQL JDBC URL")
    parser.add_argument("--db-user", default=DEFAULTS["db_user"], help="PostgreSQL user")
    parser.add_argument("--db-password", default=DEFAULTS["db_password"], help="PostgreSQL password")
    parser.add_argument("--db-schema", default=DEFAULTS["db_schema"], help="Source schema name")
    parser.add_argument("--table-name", default=DEFAULTS["table_name"], help="Source table name")
    parser.add_argument("--hdfs-base-path", default=DEFAULTS["hdfs_base_path"], help="Base HDFS path")
    parser.add_argument("--ingest-date", default=DEFAULTS["ingest_date"], help="Ingestion date (YYYY-MM-DD)")

    return parser.parse_args()

def main():
    args = parse_args()
    logger.info("Starting Bronze PostgreSQL ingestion job")
    logger.info("Table: %s.%s", args.db_schema, args.table_name)
    logger.info("Ingest date: %s", args.ingest_date)

    try:
        # --------------------------------------------------
        # Spark session
        # --------------------------------------------------
        spark = (
            SparkSession.builder
            .appName(f"bronze_postgres_{args.table_name}")
            .getOrCreate()
        )

        # --------------------------------------------------
        # JDBC read (NO business logic here)
        # --------------------------------------------------
        jdbc_table = f"{args.db_schema}.{args.table_name}"
        logger.info("Reading data from PostgreSQL via JDBC")

        df = (
            spark.read
            .format("jdbc")
            .option("url", args.jdbc_url)
            .option("dbtable", jdbc_table)
            .option("user", args.db_user)
            .option("password", args.db_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        source_count = df.count()
        logger.info("Rows read from source: %s", source_count)

        # --------------------------------------------------
        # Add technical lineage columns (ALLOWED in Bronze)
        # --------------------------------------------------
        df_bronze = (
            df
            .withColumn("ingest_date", lit(args.ingest_date))
            .withColumn("source_system", lit("postgres"))
        )

        # --------------------------------------------------
        # Target HDFS path
        # --------------------------------------------------
        output_path = (
            f"{args.hdfs_base_path}/application/"
            f"{args.table_name}/ingest_date={args.ingest_date}"
        )
        logger.info("Writing Bronze data to HDFS: %s", output_path)

        (
            df_bronze
            .write
            .mode("overwrite")
            .parquet(output_path)
        )

        logger.info("Rows written to Bronze: %s", source_count)
        logger.info("Bronze PostgreSQL ingestion completed successfully")
        spark.stop()
    except Exception:
        logger.exception("Bronze PostgreSQL ingestion failed")
        raise


if __name__ == "__main__":
    main()