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

# create a DF which calculates the following
# If the state = NY increment would be 10% of salary + 5% of bonus
# If the state = CA increment would be 12% of salary + 3% of bonus
def increment(state, salary, bonus):
  if state == "NY":
    sum = (salary * .1 + bonus * .05)
  if state == "CA":
    sum = (salary * .12 + bonus * .03)
  return sum
  
increment_UDF = udf(lambda x, y, z : increment(x, y, z), DoubleType())
state_wise_salary_inc_df = ofc_data_df.select("*") \
.withColumn("Compensation", increment_UDF(ofc_data_df.state,ofc_data_df.salary, ofc_data_df.bonus))
state_wise_salary_inc_df.show()
