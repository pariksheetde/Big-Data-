# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

ofc_data_schema = StructType([
                          StructField('employee_id', IntegerType(), True),
                          StructField('employee_name', StringType(), True),
                          StructField('department', StringType(), True),
                          StructField('state', StringType(), True),
                          StructField('salary', IntegerType(), True),
                          StructField('age', IntegerType(), True),
                          StructField('bonus', IntegerType(), True)
                         ])

ofc_data_df = spark.read.csv("/FileStore/tables/OfficeDataProject.csv", header = True, schema = ofc_data_schema)
ofc_data_df.printSchema()
ofc_data_df.show()
print("Number of records", ofc_data_df.count())

# COMMAND ----------

# print total number of employees in the company
agg_emp_cnt = ofc_data_df.select("employee_id").count()
print(agg_emp_cnt)

# COMMAND ----------

# print unique department name and their count in the company
dept_cnt_df = ofc_data_df.select("department") \
.distinct() \
.orderBy(col("department").asc())
dept_cnt_df.show()
dept_cnt_df.count()

# COMMAND ----------

# print total number of employees in each department
emp_cnt_per_dept_df = ofc_data_df.select("department", "employee_id") \
.groupBy(col("department")) \
.agg(
    count(col("employee_id")).alias("cnt_employees_per_dept")
    ) \
.sort(col("department").asc(),col("cnt_employees_per_dept").desc())
emp_cnt_per_dept_df.show()
emp_cnt_per_dept_df.count()

# COMMAND ----------

# print total number of employees in each state
agg_emp_cnt_per_state = ofc_data_df.groupBy(col("state")) \
.agg(
    count(col("employee_id")).alias("emp_per_state_cnt")
    ) \
.orderBy(col("emp_per_state_cnt").desc())
agg_emp_cnt_per_state.show()

# COMMAND ----------

# print total number of employees in each state in each dept
agg_emp_cnt_per_state_dept = ofc_data_df.groupBy(col("state"), col("department")) \
.agg(
    count(col("employee_id")).alias("emp_per_state_cnt")
    ) \
.orderBy(col("emp_per_state_cnt").desc())
agg_emp_cnt_per_state_dept.show()

# COMMAND ----------

# calculate min() max() of salary in each department and sort the salaries in asc order
agg_min_max_salary_per_dept = ofc_data_df.groupBy(col("department")) \
.agg(
    min(col("salary")).alias("min_salary"),
    max(col("salary")).alias("max_salary")
    ) \
.orderBy(col("max_salary").asc(), col("min_salary").asc())
agg_min_max_salary_per_dept.show()

# COMMAND ----------

# calculate average bonus of employees in NY state
agg_avg_bonus_NY_df = ofc_data_df.filter("state = 'NY'") \
.groupBy(col("state")) \
.agg(
    avg(col("bonus")).alias("avg_bonus_per_state")
    )
agg_avg_bonus_NY_df.show()

# COMMAND ----------

# return all the employees working in NY state under Finance department whose bonus is greater than the average bonus of employees in NY state
NY_df = ofc_data_df.select(col("employee_id"), col("employee_name"), col("department"), col("bonus"), col("state")) \
.where("department = 'Finance' and state = 'NY'") \
.filter("bonus > 1251.3468208092486")
NY_df.show(truncate = False)

# COMMAND ----------

# Raise the salary by $500 whose age is greater than 45

def hike(age, salary):
  if age > 45:
    return salary + 500
  return salary
  
hike_udf = udf(lambda x,y: hike(x,y), IntegerType())
raise_salary_df = ofc_data_df.select("*") \
.withColumn("New_CTC", hike_udf(ofc_data_df.age, ofc_data_df.salary))
raise_salary_df.show()

# COMMAND ----------

# create a df of all the employees whose age is greater than 45 and save them in a file
output_df = ofc_data_df.select("*") \
.where(col("age") > 45)
output_df.show()
output_df.write.mode("Overwrite").options(header = "True").csv("/FileStore/tables/output/Office_Data_Project")

# COMMAND ----------

# verify correct records are inserted
verify_ofc_data_df = spark.read.csv("/FileStore/tables/output/Office_Data_Project", header = True)
verify_ofc_data_df.printSchema()
verify_ofc_data_df.show()
print("Records Effected ", verify_ofc_data_df.count())
