"""
Quick preview script for Bronze and Silver data.
Run this to see sample rows from any Bronze/Silver Parquet table.
"""
import argparse
from pyspark.sql import SparkSession

def preview_parquet(spark, path: str, num_rows: int = 20):
    """Read Parquet and show sample rows."""
    print(f"\n{'='*80}")
    print(f"Previewing: {path}")
    print(f"{'='*80}\n")
    
    df = spark.read.parquet(path)
    
    print(f"Total rows: {df.count()}")
    print(f"Columns ({len(df.columns)}): {', '.join(df.columns[:10])}{'...' if len(df.columns) > 10 else ''}")
    print(f"\nSchema:")
    df.printSchema()
    print(f"\nSample rows (showing {num_rows}):")
    print("-" * 80)
    df.show(num_rows, truncate=False)
    print("-" * 80)

def main():
    parser = argparse.ArgumentParser(description="Preview Bronze/Silver Parquet data")
    parser.add_argument("--path", required=True, help="HDFS path to Parquet (e.g. hdfs://namenode:8020/raw/csv/bureau)")
    parser.add_argument("--rows", type=int, default=20, help="Number of rows to show (default: 20)")
    args = parser.parse_args()
    
    spark = SparkSession.builder.appName("preview_data").getOrCreate()
    
    try:
        preview_parquet(spark, args.path, args.rows)
    except Exception as e:
        print(f"Error reading {args.path}: {e}")
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
