# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

sales_schema = StructType([StructField('Company', StringType(), True),
                          StructField('Person', StringType(), True),
                          StructField('Sales', DoubleType(), True)
                         ])

sales_df = spark.read.csv("/FileStore/tables/sales_info.csv", header = True, schema = sales_schema)

sales_cln_df = sales_df.select(col("Company"), col("Person"), col("Sales").cast("double").alias("Sales_Amt"))
sales_cln_df.printSchema()
sales_cln_df.show()
# df.select()

# COMMAND ----------

# calculate sales for each company
agg_sum_sales_df = sales_cln_df.groupBy(col("Company")) \
.agg(
  sum(col("Sales_Amt")).alias("Tot_Sales")
    ) \
.orderBy(col("Tot_Sales").desc())
agg_sum_sales_df.show()

# COMMAND ----------

# calculate sales rank wise made by sales person in each company
from pyspark.sql.window import Window
windowSpec  = Window.partitionBy("Company").orderBy(col("Sales_Amt").desc())
top_sales_df = sales_cln_df.withColumn("Rank",rank().over(windowSpec))
top_sales_df.show(truncate=False)

# COMMAND ----------

# calculate top sales made by each sales person in each company
from pyspark.sql.window import Window
windowSpec  = Window.partitionBy("Company").orderBy(col("Sales_Amt").desc())
top_sales_df = sales_cln_df.withColumn("Rank",rank().over(windowSpec)) \
.where(col("Rank") == 1) \
.sort(col("Sales_Amt").desc())
top_sales_df.show(truncate=False)

# COMMAND ----------

# calculate worst sales made by each sales person in each company
from pyspark.sql.window import Window
windowSpec  = Window.partitionBy("Company").orderBy(col("Sales_Amt").asc())
worst_sales_df = sales_cln_df.withColumn("Rank",rank().over(windowSpec)) \
.filter(col("Rank") == 1) \
.sort(col("Sales_Amt").asc())
worst_sales_df.show(truncate=False)
