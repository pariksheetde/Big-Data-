# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions as f

if __name__ == "__main__":
    print("Accessing Word Count")

words_df = spark.read.text("/FileStore/tables/Statement.txt")
words_df.show(truncate = False)

# COMMAND ----------

fmt_words_df = words_df.withColumn("Splitted_Words",f.split("value"," "))
fmt_words_df.select("Splitted_Words").show(truncate = False)

# COMMAND ----------

df3 = fmt_words_df.withColumn("Splitted_Words", explode("Splitted_Words"))
df4 = df3.select(lower(col("Splitted_Words")).alias("Words"))
df4.show()

# COMMAND ----------

agg_words_cnt = df4.groupBy(col("Words")) \
.agg(
  count(col("Words")).alias("Count")
  ) \
.orderBy(col("Words").asc())
agg_words_cnt.show()
