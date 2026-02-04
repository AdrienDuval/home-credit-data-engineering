from pyspark.sql import SparkSession
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)

spark = (
    SparkSession.builder.appName("bronze_test_hdfs_write").getOrCreate()
)

logger.info("Spark session started")
data = [
    (1, "Alice", 25),
    (2, "Bob", 30),
    (3, "Charlie", 40)
]

columns = ["id", "name", "age"]

df = spark.createDataFrame(data,columns)
print("Dataframe_prev: ", df)

logger.info("Test DataFrame created")

output_path = "hdfs://namenode:8020/raw/test_spark_write"

df.write.mode("overwrite").parquet(output_path)

logger.info(f"Data successfully written to {output_path}")

spark.stop()

logger.info("spark session stopped")