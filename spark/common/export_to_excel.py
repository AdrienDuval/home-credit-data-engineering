"""
Export Spark DataFrame samples to Excel for visualization.
Reads Parquet from HDFS, samples rows, exports to Excel (.xlsx).

Requirements:
- pandas (usually included with PySpark)
- openpyxl or xlsxwriter (for Excel export)
  If not available, falls back to CSV export.
"""
import argparse
import os
import sys
from pyspark.sql import SparkSession

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not found. Please install: pip install pandas")
    sys.exit(1)

# Project root on path (spark/common -> spark -> project root)
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.dirname(os.path.dirname(_SCRIPT_DIR))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)


def export_parquet_to_excel(
    spark,
    hdfs_path: str,
    output_excel_path: str,
    num_rows: int = 1000,
    sample_fraction: float = None,
):
    """
    Read Parquet from HDFS, sample rows, convert to Pandas, write Excel.

    Args:
        spark: SparkSession
        hdfs_path: HDFS path to Parquet (e.g. hdfs://namenode:8020/silver/silver_client_application/year=2026/month=2/day=6)
        output_excel_path: Local path for Excel file (e.g. /opt/spark/work-dir/exports/silver_client_application_sample.xlsx)
        num_rows: Max rows to export (default 1000)
        sample_fraction: Optional fraction to sample (0.0-1.0). If None, uses num_rows.
    """
    print(f"Reading from: {hdfs_path}")
    df = spark.read.parquet(hdfs_path)

    total_rows = df.count()
    print(f"Total rows in Parquet: {total_rows}")

    # Sample data
    if sample_fraction:
        df_sample = df.sample(fraction=sample_fraction, seed=42)
        print(f"Sampled {sample_fraction*100}% of data")
    else:
        # Take first N rows (or sample if dataset is huge)
        if total_rows > num_rows * 10:
            df_sample = df.sample(fraction=min(0.1, num_rows / total_rows), seed=42).limit(num_rows)
            print(f"Sampled and limited to {num_rows} rows")
        else:
            df_sample = df.limit(num_rows)
            print(f"Taking first {num_rows} rows")

    sample_count = df_sample.count()
    print(f"Exporting {sample_count} rows to Excel...")

    # Convert to Pandas (collects to driver)
    # Note: This collects data to the driver, so use reasonable num_rows
    pdf = df_sample.toPandas()

    # Ensure output directory exists
    output_dir = os.path.dirname(output_excel_path)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    # Write Excel (try openpyxl, fallback to xlsxwriter if needed)
    try:
        pdf.to_excel(output_excel_path, index=False, engine="openpyxl")
    except ImportError:
        try:
            pdf.to_excel(output_excel_path, index=False, engine="xlsxwriter")
        except ImportError:
            # Fallback: CSV if Excel libraries not available
            csv_path = output_excel_path.replace(".xlsx", ".csv")
            pdf.to_csv(csv_path, index=False)
            print(f"WARNING: Excel libraries not found. Wrote CSV instead: {csv_path}")
            return

    print(f"Excel file written: {output_excel_path}")
    print(f"Columns: {len(pdf.columns)}")
    print(f"Rows exported: {len(pdf)}")


def main():
    parser = argparse.ArgumentParser(description="Export Parquet sample to Excel")
    parser.add_argument("--hdfs-path", required=True, help="HDFS path to Parquet")
    parser.add_argument("--output", required=True, help="Local Excel output path (.xlsx)")
    parser.add_argument("--rows", type=int, default=1000, help="Max rows to export (default: 1000)")
    parser.add_argument("--sample-fraction", type=float, default=None, help="Sample fraction (0.0-1.0), optional")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("export_to_excel").getOrCreate()

    try:
        export_parquet_to_excel(
            spark,
            args.hdfs_path,
            args.output,
            args.rows,
            args.sample_fraction,
        )
    except Exception as e:
        print(f"Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
