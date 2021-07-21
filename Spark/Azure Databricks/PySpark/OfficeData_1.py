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
                          StructField('salary', IntegerType(), True),
                          StructField('age', IntegerType(), True),
                          StructField('bonus', IntegerType(), True)
                         ])

ofc_data_df = spark.read.csv("/FileStore/tables/OfficeData.csv", header = True, schema = ofc_data_schema)
ofc_data_df.printSchema()
ofc_data_df.show()

# COMMAND ----------

# order the bonus in assc order
bonus_sort_asc_df = ofc_data_df.select("*") \
.sort(col("bonus").asc())
bonus_sort_asc_df.show()

# COMMAND ----------

# sort age in desc, bonus in asc order
age_bonus_sort_df = ofc_data_df.select("*") \
.orderBy(col("age").desc(), col("bonus").asc())
age_bonus_sort_df.show()

# COMMAND ----------

# sort age in desc, bonus in desc, salary in asc order
age_bonus_salary_sort_df = ofc_data_df.select("*") \
.orderBy(col("age").desc(), col("bonus").desc(), col("salary").asc())
age_bonus_salary_sort_df.show()
