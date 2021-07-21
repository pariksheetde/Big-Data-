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
    print("Accessing Word Count DataSet")

words_df = spark.read.csv("/FileStore/tables/WordCount.csv", header = False) \
.withColumnRenamed("_c0", "Words") \
.orderBy(col("Words").asc())
words_df.printSchema()
words_df.show(truncate = False)

# COMMAND ----------

words_cnt_df = words_df.groupBy(col("Words")) \
.agg(
  count("*").alias("cnt")
  ) \
.orderBy(col("cnt").desc())
words_cnt_df.show()
print("Records Effected: ",words_cnt_df.count())
