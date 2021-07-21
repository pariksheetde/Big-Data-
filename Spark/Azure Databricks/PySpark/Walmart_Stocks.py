# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

wmt_stocks_schema = StructType([StructField('Date', DateType(), True),
                          StructField('Open', StringType(), True),
                          StructField('High', DoubleType(), True),
                          StructField('Low', DoubleType(), True),
                          StructField('Close', DoubleType(), True),
                          StructField('Volume', DoubleType(), True),
                          StructField('Adj CLose', DoubleType(), True),
                         ])

wmt_df = spark.read.csv("/FileStore/tables/walmart_stock.csv", header = True, schema = wmt_stocks_schema)
print(wmt_df.columns)
wmt_df.printSchema()
wmt_df.show(5, truncate = False)

# COMMAND ----------

# wmt_cln_df = wmt_df.select("Date", \
#                            round("Open",2).alias("Open"), \
#                            round("High",2).alias("High"), \
#                            round("Low",2).alias("Low"), \
#                            round("Close",2).alias("Close"), \
#                            round("Volume",2).alias("Volume"), \
#                            round("Adj CLose",2).alias("Adj_Close")
#                           )
wmt_cln_df = wmt_df.select("Date", 
                           format_number(wmt_df["Open"].cast("double"),2).alias("Open"),
                           format_number(wmt_df["High"].cast("double"),2).alias("High"),
                           format_number(wmt_df["Low"].cast("double"),2).alias("Low"),
                           format_number(wmt_df["Volume"].cast("double"),2).alias("Volume"),
                           format_number(wmt_df["Adj CLose"].cast("double"),2).alias("Adj_Close"),
                          )
wmt_cln_df.show()
wmt_cln_df.printSchema()

# COMMAND ----------

# calculate ratio between High Price : Volume
HP_Vol_df = wmt_df.select(col("High"),
                          col("Volume")
                          ).withColumn("HV_Ratio", (col("High") / col("Volume")).cast("float"))
HP_Vol_df.show()

# COMMAND ----------

# calculate number of times was High greater than $80
high_80_df = wmt_df.select(round(col("High"),2).alias("High")) \
.filter(col("High") >= 80)
print("Number of times greater than $80:", high_80_df.count())
high_80_df.show()

# COMMAND ----------

# calculate what percentage of times was High greater than $80
agg_high_df = wmt_df.select(round(col("High"),2).alias("High")) \
.count()

high_80_cnt_df = wmt_df.select(col("High")) \
.filter(col("High") >= 80) \
.count()

result = (high_80_cnt_df / agg_high_df * 100)

# high_80_cnt_df.printSchema()
print(result)
