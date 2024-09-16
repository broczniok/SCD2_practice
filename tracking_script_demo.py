from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max
from datetime import datetime

spark = SparkSession.builder \
    .appName("Demo") \
    .getOrCreate()

END_TIME = '9999-12-31 23:59:59'

DIM_ID = 1
if spark.read.table("demo.iceberg.mobileapp").agg(max("dim_id")).collect()[0][0] is not None:
    DIM_ID = spark.read.table("demo.iceberg.mobileapp").agg(max("dim_id")).collect()[0][0] + 1


def check_deletion(source_row):
    dim_df = spark.read.table("demo.iceberg.mobileapp")

    source_id = source_row['userId']

    if dim_df.filter(col("userId") == source_id).count() > 0:
        spark.sql(f"""
            UPDATE demo.iceberg.mobileapp 
            SET action_cd = 'D', expired_ts = '{str(datetime.now())}' 
            WHERE userId = '{source_id}'
        """)

        spark.sql(f"""
            INSERT INTO demo.iceberg.mobileapp 
            VALUES ({DIM_ID}, '{source_id}', 
                    '{source_row['eventId']}', 
                    '{source_row['eventType']}', 
                    '{source_row['eventTime']}', 
                    '{str(source_row['attributes']).replace("'", "''")}', 
                    'D', '{str(datetime.now())}', '{END_TIME}')
        """)
        print(f"Deleted user: {source_id}")


def check_insertion(source_row):
    dim_df = spark.read.table("demo.iceberg.mobileapp")

    source_id = source_row['userId']

    if dim_df.filter(col("userId") == source_id).count() == 0:

        spark.sql(f"""
            INSERT INTO demo.iceberg.mobileapp 
            VALUES ({DIM_ID}, '{source_id}', 
                    '{source_row['eventId']}', 
                    '{source_row['eventType']}', 
                    '{source_row['eventTime']}', 
                    '{str(source_row['attributes']).replace("'", "''")}', 
                    'I', '{str(datetime.now())}', '{END_TIME}')
        """)
        print(f"Inserted user: {source_id}")


def check_update(source_row):
    dim_df = spark.read.table("demo.iceberg.mobileapp")

    source_id = source_row['userId']

    if dim_df.filter(col("userId") == source_id).count() > 0:
        existing_record = dim_df.filter(col("userId") == source_id).first()

        fields_to_compare = ['eventId', 'eventType', 'eventTime', 'attributes']
        has_changes = any(
            source_row[field] != getattr(existing_record, field) for field in fields_to_compare
        )

        if has_changes:
            spark.sql(f"""
                INSERT INTO demo.iceberg.mobileapp 
                VALUES ({DIM_ID}, '{source_row['userId']}', 
                        '{source_row['eventId']}', 
                        '{source_row['eventType']}', 
                        '{source_row['eventTime']}', 
                        '{str(source_row['attributes']).replace("'", "''")}', 
                        'U', '{str(datetime.now())}', '{END_TIME}')
            """)
            print(f"Updated user: {source_id}")

def read_parquets(file_path):
    df = spark.read.parquet(file_path)

    for row in df.collect():
        check_insertion(row)
        check_update(row)
        check_deletion(row)

    spark.stop()

if __name__ == "__main__":
    read_parquets("parquets")
    spark.table("demo.iceberg.mobileapp").show()
