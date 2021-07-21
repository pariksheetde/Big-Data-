# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

if __name__ == "__main__":
    print("Accessing Customers file")
    
cust_schema = StructType([StructField('Cust_Key', IntegerType(), True),
                          StructField('Cust_ID', IntegerType(), True),
                          StructField('F_Name', StringType(), True),
                          StructField('L_Name', StringType(), True),
                          StructField('Country', StringType(), True),
                          StructField('CTC', IntegerType(), True),
                          StructField('Baic_Pay', DoubleType(), True),
                          StructField('ZIP', IntegerType(), True),
                          StructField('Commission', DoubleType(), True)
                         ])    

null_df = spark.read.csv("/FileStore/tables/ContainsNull.csv", header = True)

null_df.show()
print("Number of partitions", null_df.rdd.getNumPartitions())

# COMMAND ----------

# drop rows that contain any NULL records
drop_null_df = null_df.na.drop()
drop_null_df.show()
