# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

apple_stocks_schema = StructType([StructField('Date', DateType(), True),
                          StructField('Open', StringType(), True),
                          StructField('High', DoubleType(), True),
                          StructField('Low', DoubleType(), True),
                          StructField('Close', DoubleType(), True),
                          StructField('Volume', DoubleType(), True),
                          StructField('Adj CLose', DoubleType(), True),
                         ])

apple_df = spark.read.csv("/FileStore/tables/appl_stock.csv", header = True, schema = apple_stocks_schema)
apple_df.printSchema()
apple_df.show()

# COMMAND ----------

date_df = apple_df.select(dayofmonth(col("Date")).alias("Day_Month"),
                          dayofweek(col("Date")).alias("Day_Week"),
                          month(col("Date")).alias("Month"),
                          year(col("Date")).alias("Year"),
                          col("Open"),
                          col("High"),
                          col("Low"),
                          col("Close"),
                          col("Volume"),
                          col("Adj CLose")
                         ) \
.withColumnRenamed("Adj CLose", "Adj_Cose")
date_df.show()
date_df.printSchema()

# COMMAND ----------

# Min, Max open for year
agg_min_open_per_year = date_df.groupBy(col("Year")) \
.agg(
  min(col("Open")).alias("Min_Open_Price"),
  max(col("Open")).alias("Max_Open_Price"),
  avg(col("Open")).alias("Avg_Open_Price")
  ) \
.sort(col("Year"))
agg_min_open_per_year.show()
