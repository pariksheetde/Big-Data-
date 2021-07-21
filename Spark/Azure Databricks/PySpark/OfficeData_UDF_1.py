# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

ofc_data_schema = StructType([StructField('employee_name', StringType(), True),
                          StructField('department', StringType(), True),
                          StructField('state', StringType(), True),
                          StructField('salary', DoubleType(), True),
                          StructField('age', IntegerType(), True),
                          StructField('bonus', DoubleType(), True)
                         ])

ofc_data_df = spark.read.csv("/FileStore/tables/OfficeData.csv", header = True, schema = ofc_data_schema)
ofc_data_df.printSchema()
ofc_data_df.show()

# COMMAND ----------

# calculate the CTC (salary + bonus)
salary_df = ofc_data_df.select("*") \
.withColumn("CTC", col("salary") + col("bonus"))
salary_df.show()

# COMMAND ----------

def CTC(salary, bonus):
  return (salary + bonus)

CTC_UDF = udf(lambda x,y : CTC(x,y), DoubleType())

CTC_df = ofc_data_df.select("*") \
.withColumn("Compensation", CTC_UDF(ofc_data_df.salary, ofc_data_df.bonus))
CTC_df.show()
