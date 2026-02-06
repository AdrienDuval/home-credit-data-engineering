"""
Gold layer processor.
Reads all four Silver tables, joins on SK_ID_CURR, builds:
  1. gold_client_risk_profile — 1 row per client with risk segment (HIGH/MEDIUM/LOW)
  2. gold_portfolio_risk — 1 row per risk_segment with counts and exposure
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
    log_file_basename="processor",
)

DEFAULT_SILVER_BASE = os.environ.get("SILVER_BASE", "hdfs://namenode:8020/silver")
DEFAULT_GOLD_BASE = os.environ.get("GOLD_BASE", "hdfs://namenode:8020/gold")
DEFAULT_INGEST_DATE = datetime.date.today().isoformat()

# Risk thresholds (rule-based, exam-friendly)
BUREAU_DEBT_RATIO_HIGH = 0.5
BUREAU_DEBT_RATIO_MEDIUM = 0.2
PAYMENT_LATE_COUNT_HIGH = 3
PAYMENT_AVG_DELAY_DAYS_HIGH = 14
PAYMENT_LATE_COUNT_MEDIUM = 1
PAYMENT_AVG_DELAY_DAYS_MEDIUM = 1
REJECTION_RATE_HIGH = 0.5
REJECTION_RATE_MEDIUM = 0.2


def parse_args():
    parser = argparse.ArgumentParser(description="Gold processor (client risk profile + portfolio risk)")
    parser.add_argument("--silver-base", default=DEFAULT_SILVER_BASE, help="Base HDFS path for Silver tables")
    parser.add_argument("--gold-base", default=DEFAULT_GOLD_BASE, help="Base HDFS path for Gold output")
    parser.add_argument("--ingest-date", default=DEFAULT_INGEST_DATE, help="Partition date YYYY-MM-DD (must match Silver)")
    return parser.parse_args()


def parse_ingest_date(ingest_date_str: str):
    dt = datetime.datetime.strptime(ingest_date_str, "%Y-%m-%d")
    return dt.year, dt.month, dt.day


def build_gold_client_risk_profile(spark, silver_base: str, gold_base: str, ingest_date: str):
    """
    Gold table 1: one row per SK_ID_CURR.
    Joins silver_client_application, silver_bureau_summary, silver_payment_behavior, silver_previous_applications.
    Computes: income, credit_exposure, bureau_debt_ratio, payment_delay_score, previous_rejection_rate,
    default_flag (TARGET), risk_segment (HIGH/MEDIUM/LOW).
    """
    year, month, day = parse_ingest_date(ingest_date)
    silver_base = silver_base.rstrip("/")
    gold_base = gold_base.rstrip("/")

    partition_suffix = f"year={year}/month={month}/day={day}"
    paths = {
        "app": f"{silver_base}/silver_client_application/{partition_suffix}",
        "bureau": f"{silver_base}/silver_bureau_summary/{partition_suffix}",
        "payment": f"{silver_base}/silver_payment_behavior/{partition_suffix}",
        "prev": f"{silver_base}/silver_previous_applications/{partition_suffix}",
    }

    logger.info("Reading Silver tables for partition %s", partition_suffix)
    df_app = spark.read.parquet(paths["app"])
    df_bureau = spark.read.parquet(paths["bureau"])
    df_payment = spark.read.parquet(paths["payment"])
    df_prev = spark.read.parquet(paths["prev"])

    n_app = df_app.count()
    n_bureau = df_bureau.count()
    n_payment = df_payment.count()
    n_prev = df_prev.count()
    logger.info("Silver row counts: application=%s, bureau_summary=%s, payment_behavior=%s, previous_applications=%s", n_app, n_bureau, n_payment, n_prev)

    # Select and rename columns to avoid duplicates after join
    df_app = df_app.select(
        F.col("SK_ID_CURR"),
        F.col("AMT_INCOME_TOTAL").alias("income"),
        F.col("AMT_CREDIT").alias("credit_exposure"),
        F.col("TARGET").alias("default_flag"),
    )
    df_bureau = df_bureau.select(
        F.col("SK_ID_CURR"),
        F.coalesce(F.col("bureau_total_debt"), F.lit(0)).alias("bureau_total_debt"),
    )
    df_payment = df_payment.select(
        F.col("SK_ID_CURR"),
        F.coalesce(F.col("payment_avg_delay_days"), F.lit(0)).alias("payment_avg_delay_days"),
        F.coalesce(F.col("payment_late_count"), F.lit(0)).alias("payment_late_count"),
    )
    df_prev = df_prev.select(
        F.col("SK_ID_CURR"),
        F.coalesce(F.col("previous_rejection_rate"), F.lit(0.0)).alias("previous_rejection_rate"),
    )

    # Left joins from application (base)
    df = df_app
    df = df.join(df_bureau, on="SK_ID_CURR", how="left")
    df = df.join(df_payment, on="SK_ID_CURR", how="left")
    df = df.join(df_prev, on="SK_ID_CURR", how="left")

    # Fill nulls from joins for numeric fields used in ratios and rules
    df = df.withColumn("bureau_total_debt", F.coalesce(F.col("bureau_total_debt"), F.lit(0)))
    df = df.withColumn("payment_avg_delay_days", F.coalesce(F.col("payment_avg_delay_days"), F.lit(0)))
    df = df.withColumn("payment_late_count", F.coalesce(F.col("payment_late_count"), F.lit(0)))
    df = df.withColumn("previous_rejection_rate", F.coalesce(F.col("previous_rejection_rate"), F.lit(0.0)))

    # bureau_debt_ratio = bureau_total_debt / credit_exposure (avoid divide by zero)
    df = df.withColumn(
        "bureau_debt_ratio",
        F.when(F.col("credit_exposure").isNotNull() & (F.col("credit_exposure") > 0),
              F.col("bureau_total_debt") / F.col("credit_exposure")).otherwise(F.lit(None)),
    )

    # Simple payment_delay_score: combine avg delay and late count (e.g. avg_delay + 5 * late_count for scale)
    df = df.withColumn(
        "payment_delay_score",
        F.col("payment_avg_delay_days") + (F.col("payment_late_count") * 5.0),
    )

    # Risk segment: HIGH if any high; MEDIUM if any medium; else LOW
    debt_high = F.coalesce(F.col("bureau_debt_ratio"), F.lit(0)) >= BUREAU_DEBT_RATIO_HIGH
    debt_medium = F.coalesce(F.col("bureau_debt_ratio"), F.lit(0)) >= BUREAU_DEBT_RATIO_MEDIUM
    late_high = (F.col("payment_late_count") >= PAYMENT_LATE_COUNT_HIGH) | (F.col("payment_avg_delay_days") >= PAYMENT_AVG_DELAY_DAYS_HIGH)
    late_medium = (F.col("payment_late_count") >= PAYMENT_LATE_COUNT_MEDIUM) | (F.col("payment_avg_delay_days") >= PAYMENT_AVG_DELAY_DAYS_MEDIUM)
    rej_high = F.col("previous_rejection_rate") >= REJECTION_RATE_HIGH
    rej_medium = F.col("previous_rejection_rate") >= REJECTION_RATE_MEDIUM

    any_high = debt_high | late_high | rej_high
    any_medium = debt_medium | late_medium | rej_medium

    df = df.withColumn(
        "risk_segment",
        F.when(any_high, F.lit("HIGH"))
        .when(any_medium, F.lit("MEDIUM"))
        .otherwise(F.lit("LOW")),
    )

    # Drop partition columns from Silver if present (keep only Gold schema)
    for c in ["year", "month", "day"]:
        if c in df.columns:
            df = df.drop(c)

    n_out = df.count()
    out_path = f"{gold_base}/gold_client_risk_profile"
    logger.info("Writing Gold to %s (partitioned by year/month/day)", out_path)
    df_gold = df.withColumn("year", F.lit(year)).withColumn("month", F.lit(month)).withColumn("day", F.lit(day))
    df_gold.write.mode("overwrite").partitionBy("year", "month", "day").parquet(out_path)

    logger.info("Gold gold_client_risk_profile written: %s rows", n_out)
    return n_out


def build_gold_portfolio_risk(spark, gold_base: str, ingest_date: str):
    """
    Gold table 2: one row per risk_segment.
    Reads gold_client_risk_profile and aggregates: number of clients, total exposure (sum credit),
    average default rate (avg TARGET for train), optional avg income.
    """
    year, month, day = parse_ingest_date(ingest_date)
    gold_base = gold_base.rstrip("/")
    partition_suffix = f"year={year}/month={month}/day={day}"
    path = f"{gold_base}/gold_client_risk_profile/{partition_suffix}"

    logger.info("Reading gold_client_risk_profile from %s", path)
    df = spark.read.parquet(path)

    n_input = df.count()
    logger.info("gold_client_risk_profile rows: %s", n_input)

    df_portfolio = (
        df.groupBy("risk_segment")
        .agg(
            F.count("*").alias("client_count"),
            F.sum(F.coalesce(F.col("credit_exposure"), F.lit(0))).alias("total_exposure"),
            F.avg(F.col("default_flag")).alias("avg_default_rate"),
            F.avg(F.col("income")).alias("avg_income"),
        )
    )

    out_path = f"{gold_base}/gold_portfolio_risk"
    logger.info("Writing Gold to %s (partitioned by year/month/day)", out_path)
    df_portfolio = (
        df_portfolio
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
    )
    df_portfolio.write.mode("overwrite").partitionBy("year", "month", "day").parquet(out_path)

    n_segments = df_portfolio.count()
    logger.info("Gold gold_portfolio_risk written: %s rows (one per risk_segment)", n_segments)
    return n_segments


def main():
    args = parse_args()
    logger.info("Gold processor start: silver_base=%s, gold_base=%s, ingest_date=%s", args.silver_base, args.gold_base, args.ingest_date)

    try:
        spark = SparkSession.builder.appName("gold_processor").getOrCreate()
        build_gold_client_risk_profile(spark, args.silver_base, args.gold_base, args.ingest_date)
        build_gold_portfolio_risk(spark, args.gold_base, args.ingest_date)
        spark.stop()
        logger.info("Gold processor completed successfully")
    except Exception:
        logger.exception("Gold processor failed")
        raise


if __name__ == "__main__":
    main()
