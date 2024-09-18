from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col
import time


def query_app():
    start_time = time.time()
    spark = SparkSession.builder \
        .appName("Extract") \
        .getOrCreate()

    filepath = "/home/iceberg/project/mobile_app_clickstream"
    filepath2 = "/home/iceberg/project/customer_data"

    output_dir = "parquets_2"

    try:
        app_read = spark.read.option("header", "true") \
            .option("inferSchema", "true") \
            .csv(filepath2)

        app_read.write.parquet(output_dir, mode="overwrite")

    except Exception as e:
        print(f"Error processing file {filepath}: {e}")

    spark.stop()
    end_time = time.time()
    elapsed_time = end_time - start_time

    print(f"Processing completed in {elapsed_time:.2f} seconds.")


if __name__ == "__main__":
    query_app()
