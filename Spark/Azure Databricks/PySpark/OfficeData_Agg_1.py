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

# calculate highest salary for each department, state
agg_dept_state = ofc_data_df.groupBy(col("state"), col("department")) \
.agg(
  max(col("salary")).alias("max_salary_per_dept_state")
  ) \
.sort(col("max_salary_per_dept_state").desc())
agg_dept_state.show()

# COMMAND ----------

# calculate count for each department, state
agg_cnt_dept_state = ofc_data_df.groupBy(col("department"), col("state")) \
.agg(
  count("employee_name").alias("cnt_per_dept_state")
  ) \
.sort(col("department").asc())
agg_cnt_dept_state.show()

# COMMAND ----------

# calculate sum of salary for each department, state
agg_dept_state = ofc_data_df.groupBy(col("department")) \
.agg(
  sum(col("salary")).alias("sum_salary_per_dept")
  ) \
.filter(col("sum_salary_per_dept") <= 350000) \
.sort(col("sum_salary_per_dept").desc())
agg_dept_state.show()
