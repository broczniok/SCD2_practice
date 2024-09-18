from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, row_number, lit, to_timestamp
from pyspark.sql import Window
from datetime import datetime

spark = SparkSession.builder \
    .appName("Tracking Changes") \
    .getOrCreate()
spark.sparkContext.setLogLevel('WARN')


END_TIME = '9999-12-31 23:59:59'
dim_df = spark.read.table("demo.iceberg.customer_dim")


def check_insertion(source_df):

    joined_df = source_df.alias('src') \
        .join(dim_df.alias('dim'), col('src.customer_id') == col('dim.customer_id'), 'left') \
        .select('src.*','dim.action_cd','dim.created_ts','dim.expired_ts' )

    new_record_df = joined_df.filter(col("action_cd").isNull())

    if new_record_df.count() > 0:
        new_records_with_customer_id = new_record_df.withColumn(
            "action_cd", lit('I')
        ).withColumn(
            "created_ts", to_timestamp(lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        ).withColumn(
            "expired_ts", to_timestamp(lit(END_TIME))
        )

        new_records_with_customer_id.select(
            "customer_id", "name", "company", "salary", "action_cd", "created_ts", "expired_ts"
        ).write.insertInto("demo.iceberg.customer_dim", overwrite=False)


def read_parquets(file_path):
    df = spark.read.parquet(file_path)

    check_insertion(df)


if __name__ == "__main__":
    read_parquets("parquets_2")
    spark.stop()

