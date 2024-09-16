from pyspark.sql.types import LongType, StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create Table") \
    .getOrCreate()

schema = StructType([
    StructField("dim_id", LongType(), True),
    StructField("userId", StringType(), True),
    StructField("eventId", StringType(), True),
    StructField("eventType", StringType(), True),
    StructField("eventTime", StringType(), True),
    StructField("attributes", StringType(), True),
    StructField("action_cd", StringType(), True),
    StructField("created_ts", StringType(), True),
    StructField("expired_ts", StringType(), True)
])

df = spark.createDataFrame([], schema)
df.writeTo("demo.iceberg.mobileapp").createOrReplace()

print(spark.catalog.tableExists("demo.iceberg.mobileapp"))
