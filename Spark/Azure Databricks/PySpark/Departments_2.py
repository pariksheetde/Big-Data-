# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("Read from Departments.csv").getOrCreate()

dept_schema = StructType([StructField('Dept_ID', IntegerType(), True),
                          StructField('Department_Name', StringType(), True),
                          StructField('Loc_ID', IntegerType(), True)])

dept_df = spark.read.csv(path = "/FileStore/tables/Departments-1.csv", header = True, schema = dept_schema) \
.withColumn("Load_TS", current_timestamp()) \
.sort(col("Dept_ID").asc()) \
.withColumnRenamed("Department_Name","Dept_Name")

final_df = dept_df.selectExpr("Dept_Name as Dept_Nm", "Dept_ID", "Loc_ID", "Load_TS")

print(final_df.columns)
final_df.printSchema()
final_df.show(truncate = False)
print("Rows Effected:" , final_df.count())

# COMMAND ----------


