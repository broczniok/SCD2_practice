from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max, row_number, lit, to_timestamp
from pyspark.sql import Window
from datetime import datetime

spark = SparkSession.builder \
    .appName("Demo") \
    .getOrCreate()

END_TIME = '9999-12-31 23:59:59'
dim_df = spark.read.table("demo.iceberg.mobileapp")
current_max_dim_id = dim_df.agg(max("dim_id")).collect()[0][0] or 0


def check_deletion(source_df):
    dim_df_selected = dim_df.select("dim_id", "userId", "eventId", "eventType", "eventTime", "attributes", "action_cd", "created_ts", "expired_ts")
    source_df_selected = source_df.select("userId", "eventId", "eventType", "eventTime", "attributes", "date")

    left_outer_join_df = dim_df_selected.alias("dim") \
        .join(source_df_selected.alias("src"),
              (col("dim.userId") == col("src.userId")) &
              (col("dim.eventId") == col("src.eventId")),
              "left_outer")

    deleted_data_df = left_outer_join_df.filter(col("src.userId").isNull()).select("dim.*")
    deleted_data_df = deleted_data_df.withColumn("action_cd", lit('D')
                                                 ).withColumn("expired_ts", to_timestamp(lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))

    deleted_data_df.select(
        "dim_id", "userId", "eventId", "eventType", "eventTime", "attributes", "action_cd", "created_ts", "expired_ts"
    ).write.insertInto("demo.iceberg.mobileapp", overwrite=True)


def check_insertion(source_df):

    joined_df = source_df.alias('src') \
        .join(dim_df.alias('dim'), col('src.userId') == col('dim.userId'), 'left') \
        .select('dim.dim_id','src.*','dim.action_cd','dim.created_ts','dim.expired_ts')

    new_record_df = joined_df.filter(col("dim_id").isNull())

    if new_record_df.count() > 0:
        window_spec = Window.orderBy("userId")
        new_records_with_dim_id = new_record_df.withColumn(
            "dim_id", row_number().over(window_spec) + lit(current_max_dim_id)
        ).withColumn(
            "action_cd", lit('I')
        ).withColumn(
            "created_ts", to_timestamp(lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        ).withColumn(
            "expired_ts", to_timestamp(lit(END_TIME))
        )

        new_records_with_dim_id.select(
            "dim_id", "userId", "eventId", "eventType", "eventTime", "attributes", "action_cd", "created_ts", "expired_ts"
        ).write.insertInto("demo.iceberg.mobileapp", overwrite=False)


def check_update(source_df):
    ...


def read_parquets(file_path):
    df = spark.read.parquet(file_path)

    check_insertion(df)
    #check_update(df)
    check_deletion(df)


if __name__ == "__main__":
    read_parquets("parquets")
    spark.stop()

