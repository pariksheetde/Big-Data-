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

cust_df = spark.read.csv("/FileStore/tables/Customers-2.csv", header = True, schema = cust_schema)

# change the datatype of ZIP from IntegerType() to StringType()
cust_df_zip = cust_df.withColumn("Code", col("ZIP").cast("String")) \
.withColumn("Destination", lit("USA")) \
.drop(col("ZIP"))

cust_df_zip.printSchema()
cust_df_zip.show(truncate = False)
print("Records Effected ", cust_df_zip.count())
print("Number of partitions", cust_df_zip.rdd.getNumPartitions())
