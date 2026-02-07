"""
Gold layer processor.
Reads all four Silver tables, joins on SK_ID_CURR, builds:
  1. gold_client_risk_profile — 1 row per client with risk segment (HIGH/MEDIUM/LOW)
  2. gold_portfolio_risk — 1 row per risk_segment with counts and exposure

================================================================================
GOLD LAYER — SIMPLE EXPLANATION (with examples)
================================================================================

WHAT IS GOLD?
-------------
Silver gave us one summary per client for each "topic" (application, bureau debt,
payment behavior, previous applications). Gold answers the business question:
"Who is risky, and how does the portfolio look by risk?"

STEP 1: gold_client_risk_profile (one row per person)
-----------------------------------------------------
We put everything we know about each client in ONE row.

  Example — Before (scattered in Silver):
    - Application:  Marie, income 50k, credit 20k, TARGET=0 (no default)
    - Bureau:       Marie, total debt 8k
    - Payment:      Marie, avg delay 2 days, 1 late payment
    - Previous:     Marie, rejection rate 0.2

  After Gold (one row for Marie):
    - income = 50k, credit_exposure = 20k, default_flag = 0
    - bureau_debt_ratio = 8k / 20k = 0.4 (debt vs new credit)
    - payment_delay_score = 2 + 5*1 = 7 (combines delay + late count)
    - previous_rejection_rate = 0.2
    - risk_segment = MEDIUM (because 0.4 >= 0.2 medium threshold, but not high)

Risk segment rules (no ML, just rules):
  - HIGH:   debt_ratio >= 0.5 OR many late payments OR rejection_rate >= 0.5
  - MEDIUM: any "medium" level (e.g. debt_ratio >= 0.2, or at least 1 late)
  - LOW:    none of the above

STEP 2: gold_portfolio_risk (one row per risk segment)
------------------------------------------------------
We stop looking at individuals and ask: "How many HIGH/MEDIUM/LOW clients do we
have, and what is our total exposure per segment?"

  Example — Input: 663,758 clients with risk_segment = HIGH, MEDIUM, or LOW.

  Output (3 rows):
    risk_segment | client_count | total_exposure | avg_default_rate | avg_income
    -------------|--------------|----------------|------------------|------------
    HIGH         | 50,000       | 1,200,000,000  | 0.12             | 180,000
    MEDIUM       | 400,000      | 8,000,000,000  | 0.05             | 220,000
    LOW          | 213,758      | 4,500,000,000  | 0.02             | 250,000

So we can say: "We have 50k high-risk clients with 1.2B exposure and 12% default
rate" — ready for reports and dashboards.
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
    parser.add_argument("--write-datamart", action="store_true", help="After Gold, write to PostgreSQL datamart schema (for Power BI)")
    parser.add_argument("--jdbc-url", default=os.environ.get("GOLD_JDBC_URL", "jdbc:postgresql://postgres:5432/home_credit"), help="JDBC URL for PostgreSQL")
    parser.add_argument("--jdbc-user", default=os.environ.get("POSTGRES_USER", "home_credit_user"), help="PostgreSQL user")
    parser.add_argument("--jdbc-password", default=os.environ.get("POSTGRES_PASSWORD", "home_credit_pwd"), help="PostgreSQL password")
    return parser.parse_args()


def parse_ingest_date(ingest_date_str: str):
    dt = datetime.datetime.strptime(ingest_date_str, "%Y-%m-%d")
    return dt.year, dt.month, dt.day


def build_gold_client_risk_profile(spark, silver_base: str, gold_base: str, ingest_date: str):
    """
    Gold table 1: one row per client. Joins all four Silver tables on SK_ID_CURR,
    then adds bureau_debt_ratio, payment_delay_score, and risk_segment (HIGH/MEDIUM/LOW).
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

    # --- Step 1a: Read the four Silver summaries (same date partition) ---
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

    # --- Step 1b: Keep only the columns we need and rename for one row per client ---
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

    # --- Step 1c: One row per client — left join everyone else (clients without bureau/payment/prev get nulls, then 0) ---
    df = df_app
    df = df.join(df_bureau, on="SK_ID_CURR", how="left")
    df = df.join(df_payment, on="SK_ID_CURR", how="left")
    df = df.join(df_prev, on="SK_ID_CURR", how="left")

    # Fill nulls from joins for numeric fields used in ratios and rules
    df = df.withColumn("bureau_total_debt", F.coalesce(F.col("bureau_total_debt"), F.lit(0)))
    df = df.withColumn("payment_avg_delay_days", F.coalesce(F.col("payment_avg_delay_days"), F.lit(0)))
    df = df.withColumn("payment_late_count", F.coalesce(F.col("payment_late_count"), F.lit(0)))
    df = df.withColumn("previous_rejection_rate", F.coalesce(F.col("previous_rejection_rate"), F.lit(0.0)))

    # --- Step 1d: Derived metrics — bureau_debt_ratio = debt / credit (avoid divide by zero) ---
    df = df.withColumn(
        "bureau_debt_ratio",
        F.when(F.col("credit_exposure").isNotNull() & (F.col("credit_exposure") > 0),
              F.col("bureau_total_debt") / F.col("credit_exposure")).otherwise(F.lit(None)),
    )

    # --- payment_delay_score: combine "how late on average" and "how many times late" into one number ---
    df = df.withColumn(
        "payment_delay_score",
        F.col("payment_avg_delay_days") + (F.col("payment_late_count") * 5.0),
    )

    # --- Step 1e: risk_segment = HIGH / MEDIUM / LOW using simple rules (no ML) ---
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

    # --- Round numeric columns for readability (Excel, Power BI, reports) ---
    # Income & credit: whole numbers. Ratios & rates: 2–3 decimals. Delays: 2 decimals (negative = early payment).
    df = df.withColumn("income", F.round(F.col("income"), 0))
    df = df.withColumn("credit_exposure", F.round(F.col("credit_exposure"), 0))
    df = df.withColumn("payment_avg_delay_days", F.round(F.col("payment_avg_delay_days"), 2))
    df = df.withColumn("payment_delay_score", F.round(F.col("payment_delay_score"), 2))
    df = df.withColumn("bureau_debt_ratio", F.round(F.col("bureau_debt_ratio"), 3))
    df = df.withColumn("previous_rejection_rate", F.round(F.col("previous_rejection_rate"), 3))

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
    Gold table 2: one row per risk_segment (HIGH, MEDIUM, LOW).
    Aggregates gold_client_risk_profile: client_count, total_exposure, avg_default_rate, avg_income.
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
    # Round for readability (Power BI, reports)
    df_portfolio = (
        df_portfolio
        .withColumn("total_exposure", F.round(F.col("total_exposure"), 0))
        .withColumn("avg_default_rate", F.round(F.col("avg_default_rate"), 3))
        .withColumn("avg_income", F.round(F.col("avg_income"), 0))
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


def write_gold_to_datamart(spark, gold_base: str, ingest_date: str, jdbc_url: str, user: str, password: str):
    """
    Write Gold tables to PostgreSQL datamart schema for Power BI / API.
    Tables: datamart.datamart_client_risk, datamart.datamart_portfolio_summary.
    Schema must exist (run docker/postgres/init/01_create_schema.sql or CREATE SCHEMA datamart;).
    """
    year, month, day = parse_ingest_date(ingest_date)
    gold_base = gold_base.rstrip("/")
    partition_suffix = f"year={year}/month={month}/day={day}"

    jdbc_options = {
        "url": jdbc_url,
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver",
    }

    # Client risk: read Gold Parquet (already rounded), drop partition cols for clean datamart table
    path_client = f"{gold_base}/gold_client_risk_profile/{partition_suffix}"
    logger.info("Reading gold_client_risk_profile from %s for datamart", path_client)
    df_client = spark.read.parquet(path_client)
    for c in ["year", "month", "day"]:
        if c in df_client.columns:
            df_client = df_client.drop(c)

    table_client = "datamart.datamart_client_risk"
    logger.info("Writing to %s (mode overwrite)", table_client)
    df_client.write.format("jdbc").mode("overwrite").options(**jdbc_options, dbtable=table_client).save()

    # Portfolio summary: same, drop partition cols
    path_portfolio = f"{gold_base}/gold_portfolio_risk/{partition_suffix}"
    logger.info("Reading gold_portfolio_risk from %s for datamart", path_portfolio)
    df_portfolio = spark.read.parquet(path_portfolio)
    for c in ["year", "month", "day"]:
        if c in df_portfolio.columns:
            df_portfolio = df_portfolio.drop(c)

    table_portfolio = "datamart.datamart_portfolio_summary"
    logger.info("Writing to %s (mode overwrite)", table_portfolio)
    df_portfolio.write.format("jdbc").mode("overwrite").options(**jdbc_options, dbtable=table_portfolio).save()

    logger.info("Datamart write completed: %s, %s", table_client, table_portfolio)


def main():
    args = parse_args()
    logger.info("Gold processor start: silver_base=%s, gold_base=%s, ingest_date=%s", args.silver_base, args.gold_base, args.ingest_date)

    try:
        spark = SparkSession.builder.appName("gold_processor").getOrCreate()
        build_gold_client_risk_profile(spark, args.silver_base, args.gold_base, args.ingest_date)
        build_gold_portfolio_risk(spark, args.gold_base, args.ingest_date)
        if getattr(args, "write_datamart", False):
            logger.info("Writing Gold to PostgreSQL datamart (for Power BI)")
            write_gold_to_datamart(
                spark, args.gold_base, args.ingest_date,
                args.jdbc_url, args.jdbc_user, args.jdbc_password,
            )
        spark.stop()
        logger.info("Gold processor completed successfully")
    except Exception:
        logger.exception("Gold processor failed")
        raise


if __name__ == "__main__":
    main()
