# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("Read from Departments.csv").getOrCreate()

dept_df = spark.read.csv("/FileStore/tables/Departments-1.csv")
w = Window().orderBy("Dept_ID")
clean_df = dept_df.withColumnRenamed("_c0", "Dept_Id").withColumnRenamed("_c1", "Dept_Name").withColumnRenamed("_c2", "Loc_ID") \
.withColumn("RN",row_number().over(w)) \
.filter("Dept_ID <> 'Dept_ID'")
clean_df.show(truncate = False)

# COMMAND ----------


