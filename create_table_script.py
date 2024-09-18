from pyspark.sql.types import LongType, StructType, StructField, StringType
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create Table") \
    .getOrCreate()


spark.sql("""
CREATE OR REPLACE TABLE demo.iceberg.mobileapp (
    dim_id INT,  
    userId VARCHAR(255) NOT NULL,              
    eventId VARCHAR(255) NOT NULL,             
    eventType VARCHAR(100),                    
    eventTime TIMESTAMP,                       
    attributes VARCHAR(255),                           
    action_cd CHAR(1),                         
    created_ts TIMESTAMP, 
    expired_ts TIMESTAMP                       
);

""")

spark.stop()
