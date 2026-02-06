"""
Silver layer processor — Step 1: silver_client_application only.
Reads Bronze Postgres application data, applies validation rules, writes curated Silver table.
We add more Silver tables in later steps.
"""
import argparse
import os
import sys
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType
from pyspark.sql.window import Window

# Project root on path for spark.common.logger
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_SCRIPT_DIR))  # spark/silver -> spark -> project root
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

from spark.common.logger import setup_logger  # noqa: E402

logger = setup_logger(
    __name__,
    log_dir="logs/silver",
    log_file_basename="processor",
)

# Default paths (match Bronze feeder output)
DEFAULT_BRONZE_POSTGRES_BASE = os.environ.get(
    "BRONZE_POSTGRES_BASE", "hdfs://namenode:8020/raw/postgres"
)
DEFAULT_BRONZE_CSV_BASE = os.environ.get(
    "BRONZE_CSV_BASE", "hdfs://namenode:8020/raw/csv"
)
DEFAULT_SILVER_BASE = os.environ.get("SILVER_BASE", "hdfs://namenode:8020/silver")
DEFAULT_INGEST_DATE = datetime.date.today().isoformat()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Silver processor (steps 1–4: client_application, bureau_summary, payment_behavior, previous_applications)"
    )
    parser.add_argument(
        "--bronze-postgres-base",
        default=DEFAULT_BRONZE_POSTGRES_BASE,
        help="Base HDFS path for Bronze Postgres data (application_train/test)",
    )
    parser.add_argument(
        "--bronze-csv-base",
        default=DEFAULT_BRONZE_CSV_BASE,
        help="Base HDFS path for Bronze CSV data (bureau, bureau_balance, etc.)",
    )
    parser.add_argument(
        "--silver-base",
        default=DEFAULT_SILVER_BASE,
        help="Base HDFS path for Silver output",
    )
    parser.add_argument(
        "--ingest-date",
        default=DEFAULT_INGEST_DATE,
        help="Partition date YYYY-MM-DD for Silver output",
    )
    return parser.parse_args()


def parse_ingest_date(ingest_date_str: str):
    """Return (year, month, day) for partition columns."""
    dt = datetime.datetime.strptime(ingest_date_str, "%Y-%m-%d")
    return dt.year, dt.month, dt.day


def build_silver_client_application(spark, bronze_base: str, silver_base: str, ingest_date: str):
    """
    Build Silver table: silver_client_application (1 row per SK_ID_CURR).
    Reads Bronze application_train + application_test, validates, writes to Silver.
    """
    year, month, day = parse_ingest_date(ingest_date)
    bronze_base = bronze_base.rstrip("/")

    # -------------------------------------------------------------------------
    # Read Bronze (Parquet from feeder_postgres)
    # -------------------------------------------------------------------------
    path_train = f"{bronze_base}/application/application_train"
    path_test = f"{bronze_base}/application/application_test"
    logger.info("Reading Bronze application_train from %s", path_train)
    logger.info("Reading Bronze application_test from %s", path_test)

    df_train = spark.read.parquet(path_train)
    df_test = spark.read.parquet(path_test)
    total_train = df_train.count()
    total_test = df_test.count()
    logger.info("Bronze rows: application_train=%s, application_test=%s", total_train, total_test)

    # Union (same schema)
    df = df_train.unionByName(df_test, allowMissingColumns=True)
    total_before = df.count()
    logger.info("Union total rows: %s", total_before)

    # -------------------------------------------------------------------------
    # Light cleaning: cast key numerics (safe cast, leave null if invalid)
    # -------------------------------------------------------------------------
    for col_name, dtype in [
        ("AMT_INCOME_TOTAL", DoubleType()),
        ("AMT_CREDIT", DoubleType()),
        ("AMT_ANNUITY", DoubleType()),
    ]:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(dtype))

    # Normalize DAYS_BIRTH to numeric if present
    if "DAYS_BIRTH" in df.columns:
        df = df.withColumn("DAYS_BIRTH", F.col("DAYS_BIRTH").cast(IntegerType()))

    # Normalize CODE_GENDER: map XNA -> 'Unknown' for downstream use
    if "CODE_GENDER" in df.columns:
        df = df.withColumn(
            "CODE_GENDER",
            F.when(F.col("CODE_GENDER") == "XNA", F.lit("Unknown")).otherwise(
                F.col("CODE_GENDER")
            ),
        )

    # -------------------------------------------------------------------------
    # Validation rules — we log fail count for each, then keep only valid
    # -------------------------------------------------------------------------
    # Rule 1: SK_ID_CURR is not null
    rule1_ok = F.col("SK_ID_CURR").isNotNull()
    # Rule 2: AMT_INCOME_TOTAL > 0
    rule2_ok = (F.col("AMT_INCOME_TOTAL").isNotNull()) & (F.col("AMT_INCOME_TOTAL") > 0)
    # Rule 3: AMT_CREDIT > 0
    rule3_ok = (F.col("AMT_CREDIT").isNotNull()) & (F.col("AMT_CREDIT") > 0)
    # Rule 4: Applicant age >= 18 (DAYS_BIRTH is typically negative = days since birth)
    age_ok = (F.abs(F.col("DAYS_BIRTH")) / 365.0) >= 18
    rule4_ok = F.col("DAYS_BIRTH").isNotNull() & age_ok
    # Rule 5: AMT_ANNUITY is null OR AMT_ANNUITY > 0
    rule5_ok = F.col("AMT_ANNUITY").isNull() | (F.col("AMT_ANNUITY") > 0)
    # Rule 6: AMT_CREDIT >= AMT_ANNUITY when both non-null
    rule6_ok = (
        F.col("AMT_CREDIT").isNull()
        | F.col("AMT_ANNUITY").isNull()
        | (F.col("AMT_CREDIT") >= F.col("AMT_ANNUITY"))
    )
    # Rule 7: CODE_GENDER in ('M','F','Unknown') after normalization
    rule7_ok = F.col("CODE_GENDER").isin("M", "F", "Unknown")

    fail1 = df.filter(~rule1_ok).count()
    fail2 = df.filter(~rule2_ok).count()
    fail3 = df.filter(~rule3_ok).count()
    fail4 = df.filter(~rule4_ok).count()
    fail5 = df.filter(~rule5_ok).count()
    fail6 = df.filter(~rule6_ok).count()
    fail7 = df.filter(~rule7_ok).count()
    logger.info(
        "Validation failures: SK_ID_CURR null=%s, AMT_INCOME_TOTAL invalid=%s, "
        "AMT_CREDIT invalid=%s, age<18=%s, AMT_ANNUITY invalid=%s, "
        "AMT_CREDIT<AMT_ANNUITY=%s, CODE_GENDER invalid=%s",
        fail1,
        fail2,
        fail3,
        fail4,
        fail5,
        fail6,
        fail7,
    )

    valid = rule1_ok & rule2_ok & rule3_ok & rule4_ok & rule5_ok & rule6_ok & rule7_ok
    df_valid = df.filter(valid)
    total_valid = df_valid.count()
    logger.info(
        "Rows passing all 7 validation rules: %s (dropped %s)",
        total_valid,
        total_before - total_valid,
    )

    # -------------------------------------------------------------------------
    # Add partition columns and write to Silver
    # -------------------------------------------------------------------------
    df_silver = (
        df_valid
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
    )
    output_path = f"{silver_base.rstrip('/')}/silver_client_application"
    logger.info("Writing Silver to %s (partitioned by year/month/day)", output_path)

    df_silver.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_path)

    logger.info("Silver silver_client_application written: %s rows", total_valid)
    return total_valid


def build_silver_bureau_summary(
    spark, bronze_csv_base: str, silver_base: str, ingest_date: str
):
    """
    Build Silver table: silver_bureau_summary (client-level features from bureau data).

    WHAT THIS DOES (simple explanation):
    ====================================
    Imagine you're summarizing someone's loan history into one summary line.

    - "bureau" = list of loans a person has (e.g., "John has 3 loans: car loan, credit card, personal loan")
    - "bureau_balance" = monthly updates for each loan (e.g., "Car loan: Jan=paid on time, Feb=late, Mar=paid on time")

    PROBLEM: We have MANY monthly rows per loan, but we only want the LATEST status.
    SOLUTION: Use a window function to pick the newest month for each loan, then summarize per person.

    REAL-WORLD ANALOGY:
    ===================
    - Bureau = "John has 3 loans: Car loan, Credit card, Personal loan"
    - Bureau balance = monthly snapshots: "Car loan: Jan=on time, Feb=late, Mar=on time"
    - Window function = "For each loan, keep only the latest month (March)"
    - Join = "Attach March status to each loan record"
    - Aggregate = "Summarize per person: John has 3 loans, 2 active, total owed $50k, max 30 days late"

    Steps:
    1. Read Bronze bureau + bureau_balance from HDFS (Parquet).
    2. Use a window function over SK_ID_BUREAU, ordered by MONTHS_BALANCE desc,
       to get the *latest* status per bureau credit.
    3. Aggregate to client level (SK_ID_CURR) with counts and debt metrics.
    """
    year, month, day = parse_ingest_date(ingest_date)
    bronze_csv_base = bronze_csv_base.rstrip("/")

    # -------------------------------------------------------------------------
    # STEP 1: Read Bronze CSV-based tables
    # -------------------------------------------------------------------------
    # What we're reading:
    # - bureau: One row per loan (e.g., "Loan ID 123 belongs to person 456")
    # - bureau_balance: Many rows per loan (one per month: Jan, Feb, Mar...)
    #
    # Example bureau_balance data:
    #   Loan-123 | Month Jan | Status: Paid on time
    #   Loan-123 | Month Feb | Status: Paid late
    #   Loan-123 | Month Mar | Status: Paid on time  <- This is the LATEST
    #   Loan-456 | Month Jan | Status: Paid on time
    #   Loan-456 | Month Feb | Status: Paid on time  <- This is the LATEST
    #
    # We want to keep ONLY the latest month for each loan.
    # -------------------------------------------------------------------------
    path_bureau = f"{bronze_csv_base}/bureau"
    path_bureau_balance = f"{bronze_csv_base}/bureau_balance"
    logger.info("Reading Bronze bureau from %s", path_bureau)
    logger.info("Reading Bronze bureau_balance from %s", path_bureau_balance)

    df_bureau = spark.read.parquet(path_bureau)
    df_balance = spark.read.parquet(path_bureau_balance)

    logger.info(
        "Bronze rows: bureau=%s, bureau_balance=%s",
        df_bureau.count(),
        df_balance.count(),
    )

    # -------------------------------------------------------------------------
    # STEP 2: Window function - Get LATEST status per loan
    # -------------------------------------------------------------------------
    # WHAT IS A WINDOW FUNCTION?
    # ===========================
    # A window function lets you look at rows "around" the current row within a group.
    # Think of it like: "For each loan, look at all its monthly records, order them newest-first,
    # then assign row numbers: 1=newest, 2=older, 3=oldest..."
    #
    # Window definition:
    #   partitionBy("SK_ID_BUREAU")  → Group rows by loan ID
    #   orderBy(MONTHS_BALANCE desc) → Within each loan, order months newest-first
    #
    # Example with Loan-123:
    #   Before window:
    #     Loan-123 | Month -3 (March) | Status: Paid on time
    #     Loan-123 | Month -4 (Feb)   | Status: Paid late
    #     Loan-123 | Month -5 (Jan)   | Status: Paid on time
    #
    #   After window + row_number():
    #     Loan-123 | Month -3 | Status: Paid on time | rn=1  ← NEWEST (keep this!)
    #     Loan-123 | Month -4 | Status: Paid late    | rn=2
    #     Loan-123 | Month -5 | Status: Paid on time | rn=3
    #
    #   After filter(rn == 1):
    #     Loan-123 | Month -3 | Status: Paid on time  ← Only this row remains
    #
    # Result: One row per loan with its LATEST monthly status.
    # -------------------------------------------------------------------------
    w_latest = Window.partitionBy("SK_ID_BUREAU").orderBy(F.col("MONTHS_BALANCE").desc())

    df_balance_latest = (
        df_balance
        .withColumn("rn", F.row_number().over(w_latest))  # Assign row numbers: 1=newest, 2=older, etc.
        .filter(F.col("rn") == 1)  # Keep only row #1 (the newest month for each loan)
        .drop("rn")  # Remove the row number column (we don't need it anymore)
    )

    # -------------------------------------------------------------------------
    # STEP 3: Join latest status back to bureau records
    # -------------------------------------------------------------------------
    # Now we attach the latest monthly status to each loan record.
    #
    # Example:
    #   Bureau table:        Latest balance:
    #   Loan-123 | Person-456  +  Loan-123 | Month -3 | Status: Paid on time
    #   Loan-456 | Person-456  +  Loan-456 | Month -2 | Status: Paid on time
    #
    #   Result (joined):
    #   Loan-123 | Person-456 | Month -3 | Status: Paid on time
    #   Loan-456 | Person-456 | Month -2 | Status: Paid on time
    # -------------------------------------------------------------------------
    df_joined = df_bureau.join(
        df_balance_latest,
        on="SK_ID_BUREAU",
        how="left",
    )

    # -------------------------------------------------------------------------
    # STEP 4: Aggregate to person level (SK_ID_CURR)
    # -------------------------------------------------------------------------
    # Now we group by person and summarize:
    #
    # Example: Person-456 has 2 loans (Loan-123 and Loan-456)
    #
    # Before aggregation (one row per loan):
    #   Person-456 | Loan-123 | Active | Debt: $10k | Days late: 0
    #   Person-456 | Loan-456 | Active | Debt: $5k  | Days late: 30
    #
    # After aggregation (one row per person):
    #   Person-456 | Total loans: 2 | Active loans: 2 | Total debt: $15k | Max days late: 30
    #
    # Metrics we compute:
    # - bureau_credit_count: How many loans does this person have?
    # - bureau_active_credit_count: How many are currently active?
    # - bureau_total_debt: Sum of all debt amounts
    # - bureau_max_days_overdue: Worst case - what's the maximum days late across all loans?
    # - bureau_total_overdue: Sum of all overdue amounts
    # -------------------------------------------------------------------------
    agg = (
        df_joined.groupBy("SK_ID_CURR")  # Group by person ID
        .agg(
            F.count("*").alias("bureau_credit_count"),  # Count: How many loans total?
            F.sum(
                F.when(F.col("CREDIT_ACTIVE") == "Active", 1).otherwise(0)
            ).alias("bureau_active_credit_count"),  # Count: How many are "Active"?
            F.sum("AMT_CREDIT_SUM_DEBT").alias("bureau_total_debt"),  # Sum: Total debt
            F.max("CREDIT_DAY_OVERDUE").alias("bureau_max_days_overdue"),  # Max: Worst days late
            F.sum("AMT_CREDIT_SUM_OVERDUE").alias("bureau_total_overdue"),  # Sum: Total overdue
        )
    )

    total_clients = agg.count()
    logger.info("silver_bureau_summary: client rows before validation = %s", total_clients)

    # Simple validation: SK_ID_CURR not null
    rule_sk_ok = F.col("SK_ID_CURR").isNotNull()
    fail_sk = agg.filter(~rule_sk_ok).count()
    logger.info("silver_bureau_summary: SK_ID_CURR null rows = %s", fail_sk)

    df_valid = agg.filter(rule_sk_ok)
    total_valid = df_valid.count()
    logger.info(
        "silver_bureau_summary: valid client rows = %s (dropped %s)",
        total_valid,
        total_clients - total_valid,
    )

    # Add partition columns and write to Silver
    df_silver = (
        df_valid
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
    )
    output_path = f"{silver_base.rstrip('/')}/silver_bureau_summary"
    logger.info("Writing Silver to %s (partitioned by year/month/day)", output_path)

    df_silver.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
        output_path
    )

    logger.info("Silver silver_bureau_summary written: %s rows", total_valid)
    return total_valid


def build_silver_payment_behavior(
    spark, bronze_csv_base: str, silver_base: str, ingest_date: str
):
    """
    Build Silver table: silver_payment_behavior (client-level payment indicators).

    WHAT THIS DOES (simple explanation):
    ====================================
    Summarizes how each client has paid their previous installments: on time, late, total paid, etc.

    REAL-WORLD ANALOGY:
    ===================
    - installments_payments = "John paid installment 1 on time, installment 2 five days late, installment 3 two days early..."
    - This function summarizes: "John: average delay 1 day, 2 late payments, total paid $X, payment ratio 1.02"

    Steps:
    1. Read Bronze installments_payments from HDFS.
    2. Compute delay = DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT (positive = late, negative = early).
    3. Window: per client (SK_ID_CURR), order by DAYS_ENTRY_PAYMENT (client payment timeline).
    4. Aggregate per client: avg delay, count late payments, total paid, payment ratio.
    """
    year, month, day = parse_ingest_date(ingest_date)
    bronze_csv_base = bronze_csv_base.rstrip("/")

    # -------------------------------------------------------------------------
    # STEP 1: Read Bronze installments_payments
    # -------------------------------------------------------------------------
    # One row per payment (per installment of a previous loan).
    # Columns: SK_ID_PREV, SK_ID_CURR, NUM_INSTALMENT_VERSION, NUM_INSTALMENT_NUMBER,
    #          DAYS_INSTALMENT (when due), DAYS_ENTRY_PAYMENT (when actually paid),
    #          AMT_INSTALMENT (due amount), AMT_PAYMENT (amount paid)
    # -------------------------------------------------------------------------
    path_inst = f"{bronze_csv_base}/installments_payments"
    logger.info("Reading Bronze installments_payments from %s", path_inst)

    df_inst = spark.read.parquet(path_inst)
    total_rows = df_inst.count()
    logger.info("Bronze rows: installments_payments=%s", total_rows)

    # Cache after expensive read (plan requirement: cache/persist after large reads)
    df_inst = df_inst.cache()
    df_inst.count()  # trigger cache
    logger.info("Cached installments_payments for reuse")

    # -------------------------------------------------------------------------
    # STEP 2: Compute delay (positive = late, negative = early)
    # -------------------------------------------------------------------------
    # delay = DAYS_ENTRY_PAYMENT - DAYS_INSTALMENT
    # Example: due on day 30, paid on day 35 -> delay = 5 (5 days late)
    #          due on day 30, paid on day 28 -> delay = -2 (2 days early)
    # -------------------------------------------------------------------------
    df_with_delay = df_inst.withColumn(
        "payment_delay_days",
        F.coalesce(
            F.col("DAYS_ENTRY_PAYMENT").cast("int") - F.col("DAYS_INSTALMENT").cast("int"),
            F.lit(0),
        ),
    )

    # -------------------------------------------------------------------------
    # STEP 3: Window function — client payment timeline
    # -------------------------------------------------------------------------
    # For each client, order payments by when they were made (DAYS_ENTRY_PAYMENT).
    # This gives a "timeline" of payments per client (e.g. for trend or last-N analysis).
    # Example: Person-456 has payments at days 30, 60, 90 -> row_number 1, 2, 3
    # -------------------------------------------------------------------------
    window_client_timeline = Window.partitionBy("SK_ID_CURR").orderBy(
        F.col("DAYS_ENTRY_PAYMENT").asc_nulls_last()
    )
    df_with_sequence = df_with_delay.withColumn(
        "payment_sequence", F.row_number().over(window_client_timeline)
    )

    # -------------------------------------------------------------------------
    # STEP 4: Aggregate to client level (SK_ID_CURR)
    # -------------------------------------------------------------------------
    # Per client we compute:
    # - payment_avg_delay_days: average delay (positive = often late)
    # - payment_late_count: number of installments paid late (delay > 0)
    # - payment_total_paid: sum of AMT_PAYMENT
    # - payment_total_installment: sum of AMT_INSTALMENT (what was due)
    # - payment_ratio: total_paid / total_installment (e.g. 1.0 = paid exactly what was due)
    # -------------------------------------------------------------------------
    df_agg = (
        df_with_sequence.groupBy("SK_ID_CURR")
        .agg(
            F.avg("payment_delay_days").alias("payment_avg_delay_days"),
            F.sum(F.when(F.col("payment_delay_days") > 0, 1).otherwise(0)).alias(
                "payment_late_count"
            ),
            F.sum(F.coalesce(F.col("AMT_PAYMENT"), F.lit(0))).alias("payment_total_paid"),
            F.sum(F.coalesce(F.col("AMT_INSTALMENT"), F.lit(0))).alias(
                "payment_total_installment"
            ),
        )
    )

    # Payment ratio = total_paid / total_installment (avoid divide by zero)
    df_agg = df_agg.withColumn(
        "payment_ratio",
        F.when(
            F.col("payment_total_installment") > 0,
            F.col("payment_total_paid") / F.col("payment_total_installment"),
        ).otherwise(F.lit(None)),
    )

    total_clients = df_agg.count()
    logger.info("silver_payment_behavior: client rows before validation = %s", total_clients)

    # Validation: SK_ID_CURR not null
    rule_sk_ok = F.col("SK_ID_CURR").isNotNull()
    fail_sk = df_agg.filter(~rule_sk_ok).count()
    logger.info("silver_payment_behavior: SK_ID_CURR null rows = %s", fail_sk)

    df_valid = df_agg.filter(rule_sk_ok)
    total_valid = df_valid.count()
    logger.info(
        "silver_payment_behavior: valid client rows = %s (dropped %s)",
        total_valid,
        total_clients - total_valid,
    )

    # Add partition columns and write to Silver
    df_silver = (
        df_valid
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
    )
    output_path = f"{silver_base.rstrip('/')}/silver_payment_behavior"
    logger.info("Writing Silver to %s (partitioned by year/month/day)", output_path)

    df_silver.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
        output_path
    )

    df_inst.unpersist()
    logger.info("Silver silver_payment_behavior written: %s rows", total_valid)
    return total_valid


def build_silver_previous_applications(
    spark, bronze_csv_base: str, silver_base: str, ingest_date: str
):
    """
    Build Silver table: silver_previous_applications (client-level application history summary).

    WHAT THIS DOES (simple explanation):
    ====================================
    Summarizes how many times a person has applied for loans before, and what happened.

    REAL-WORLD ANALOGY:
    ===================
    - previous_application = "John applied 3 times before: Loan 1 (rejected), Loan 2 (approved), Loan 3 (approved)"
    - This function summarizes: "John has 3 previous applications, rejection rate = 33%, avg requested $10k, avg granted $8k"

    Steps:
    1. Read Bronze previous_application from HDFS (Parquet).
    2. Aggregate per client (SK_ID_CURR):
       - Count total previous applications
       - Calculate rejection rate (rejected / total)
       - Average requested amount vs average granted amount
       - Distribution by contract status (how many of each type)
    """
    year, month, day = parse_ingest_date(ingest_date)
    bronze_csv_base = bronze_csv_base.rstrip("/")

    # -------------------------------------------------------------------------
    # STEP 1: Read Bronze previous_application
    # -------------------------------------------------------------------------
    # What we're reading:
    # - previous_application: One row per previous loan application
    #   Example:
    #     Person-456 | Application-1 | Status: Rejected | Requested: $10k | Granted: $0
    #     Person-456 | Application-2 | Status: Approved | Requested: $15k | Granted: $12k
    #     Person-456 | Application-3 | Status: Approved | Requested: $8k  | Granted: $8k
    #
    # We want to summarize this into one row per person.
    # -------------------------------------------------------------------------
    path_prev = f"{bronze_csv_base}/previous_application"
    logger.info("Reading Bronze previous_application from %s", path_prev)

    df_prev = spark.read.parquet(path_prev)
    total_prev = df_prev.count()
    logger.info("Bronze rows: previous_application=%s", total_prev)

    # -------------------------------------------------------------------------
    # STEP 2: Aggregate to client level (SK_ID_CURR)
    # -------------------------------------------------------------------------
    # For each person, compute:
    # - How many previous applications?
    # - How many were rejected? (rejection rate = rejected / total)
    # - Average requested amount
    # - Average granted amount
    # - Distribution by contract status (counts per status type)
    #
    # Example: Person-456 has 3 applications
    #   Before aggregation (3 rows):
    #     Person-456 | App-1 | Rejected | Requested: $10k | Granted: $0
    #     Person-456 | App-2 | Approved | Requested: $15k | Granted: $12k
    #     Person-456 | App-3 | Approved | Requested: $8k  | Granted: $8k
    #
    #   After aggregation (1 row):
    #     Person-456 | Total apps: 3 | Rejection rate: 33% | Avg requested: $11k | Avg granted: $6.67k
    # -------------------------------------------------------------------------
    # Count total applications
    df_counts = (
        df_prev.groupBy("SK_ID_CURR")
        .agg(
            F.count("*").alias("previous_app_count"),
            # Count rejected (assuming NAME_CONTRACT_STATUS contains rejection info)
            # Common values: "Refused", "Approved", "Canceled", etc.
            F.sum(
                F.when(
                    F.col("NAME_CONTRACT_STATUS").isin("Refused", "Refused by client"),
                    1,
                ).otherwise(0)
            ).alias("previous_rejected_count"),
        )
    )

    # Calculate rejection rate
    df_counts = df_counts.withColumn(
        "previous_rejection_rate",
        F.when(F.col("previous_app_count") > 0, F.col("previous_rejected_count") / F.col("previous_app_count")).otherwise(
            F.lit(0.0)
        ),
    )

    # Amount aggregations (if columns exist)
    amount_cols = {}
    if "AMT_APPLICATION" in df_prev.columns:
        amount_cols["previous_avg_requested"] = F.avg("AMT_APPLICATION")
    if "AMT_CREDIT" in df_prev.columns:
        amount_cols["previous_avg_granted"] = F.avg("AMT_CREDIT")

    df_amounts = df_prev.groupBy("SK_ID_CURR").agg(*[amount_cols[k].alias(k) for k in amount_cols])

    # Contract status distribution (count per status type)
    # Pivot to get counts per status
    if "NAME_CONTRACT_STATUS" in df_prev.columns:
        # First, get counts per client and status
        status_grouped = (
            df_prev.groupBy("SK_ID_CURR", "NAME_CONTRACT_STATUS")
            .agg(F.count("*").alias("status_count"))
        )
        # Pivot: one column per status value
        status_counts = (
            status_grouped.groupBy("SK_ID_CURR")
            .pivot("NAME_CONTRACT_STATUS")
            .sum("status_count")
            .fillna(0)
        )
        # Rename columns to avoid conflicts (sanitize status names)
        for col_name in status_counts.columns:
            if col_name != "SK_ID_CURR":
                clean_name = col_name.lower().replace(" ", "_").replace("-", "_")
                status_counts = status_counts.withColumnRenamed(
                    col_name, f"prev_status_{clean_name}_count"
                )
    else:
        # If column doesn't exist, just create a distinct SK_ID_CURR DataFrame
        status_counts = df_prev.select("SK_ID_CURR").distinct()

    # Join all aggregations
    df_result = df_counts
    if amount_cols:
        df_result = df_result.join(df_amounts, on="SK_ID_CURR", how="left")
    df_result = df_result.join(status_counts, on="SK_ID_CURR", how="left")

    total_clients = df_result.count()
    logger.info("silver_previous_applications: client rows before validation = %s", total_clients)

    # Validation: SK_ID_CURR not null
    rule_sk_ok = F.col("SK_ID_CURR").isNotNull()
    fail_sk = df_result.filter(~rule_sk_ok).count()
    logger.info("silver_previous_applications: SK_ID_CURR null rows = %s", fail_sk)

    df_valid = df_result.filter(rule_sk_ok)
    total_valid = df_valid.count()
    logger.info(
        "silver_previous_applications: valid client rows = %s (dropped %s)",
        total_valid,
        total_clients - total_valid,
    )

    # Add partition columns and write to Silver
    df_silver = (
        df_valid
        .withColumn("year", F.lit(year))
        .withColumn("month", F.lit(month))
        .withColumn("day", F.lit(day))
    )
    output_path = f"{silver_base.rstrip('/')}/silver_previous_applications"
    logger.info("Writing Silver to %s (partitioned by year/month/day)", output_path)

    df_silver.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
        output_path
    )

    logger.info("Silver silver_previous_applications written: %s rows", total_valid)
    return total_valid


def main():
    args = parse_args()
    logger.info("Silver processor (steps 1–4: client_application, bureau_summary, payment_behavior, previous_applications)")
    logger.info("Bronze Postgres base: %s", args.bronze_postgres_base)
    logger.info("Bronze CSV base: %s", args.bronze_csv_base)
    logger.info("Silver base: %s", args.silver_base)
    logger.info("Ingest date: %s", args.ingest_date)

    try:
        spark = (
            SparkSession.builder
            .appName("silver_processor")
            .getOrCreate()
        )
        # Step 1: client application Silver
        build_silver_client_application(
            spark,
            args.bronze_postgres_base,
            args.silver_base,
            args.ingest_date,
        )
        # Step 2: bureau summary Silver
        build_silver_bureau_summary(
            spark,
            args.bronze_csv_base,
            args.silver_base,
            args.ingest_date,
        )
        # Step 3: payment behavior Silver
        build_silver_payment_behavior(
            spark,
            args.bronze_csv_base,
            args.silver_base,
            args.ingest_date,
        )
        # Step 4: previous applications Silver
        build_silver_previous_applications(
            spark,
            args.bronze_csv_base,
            args.silver_base,
            args.ingest_date,
        )
        spark.stop()
        logger.info("Silver processor completed successfully")
    except Exception:
        logger.exception("Silver processor failed")
        raise


if __name__ == "__main__":
    main()
