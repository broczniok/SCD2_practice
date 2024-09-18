from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Create Table 2") \
    .getOrCreate()


spark.sql("""
CREATE OR REPLACE TABLE demo.iceberg.customer_dim (
    customer_id INT NOT NULL,              
    name VARCHAR(255) NOT NULL,             
    company VARCHAR(255),                    
    salary FLOAT,                                                  
    action_cd CHAR(1),                         
    created_ts TIMESTAMP, 
    expired_ts TIMESTAMP                       
);

""")


spark.stop()
