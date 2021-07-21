# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

if __name__ == "__main__":
    print("Accessing Employees file")

emp_sch = StructType([StructField('EMPLOYEE_ID', IntegerType(), True),
                          StructField('FIRST_NAME', StringType(), True),
                          StructField('LAST_NAME', StringType(), True),
                          StructField('EMAIL', StringType(), True),
                          StructField('PHONE_NUMBER', StringType(), True),
                          StructField('HIRE_DATE', StringType(), True),
                          StructField('JOB_ID', StringType(), True),
                          StructField('SALARY', DoubleType(), True),
                          StructField('COMMISSION_PCT', DoubleType(), True),
                          StructField('MANAGER_ID', IntegerType(), True),
                          StructField('DEPARTMENT_ID', IntegerType(), True)
                         ])

emp_df.printSchema()
emp_df = spark.read.csv("/FileStore/tables/employees-1.csv", header = True, schema = emp_sch)
emp_df = emp_df.select("*").withColumn("Load_TS", current_timestamp())
emp_df.show()
print("Number of Partitions ", emp_df.rdd.getNumPartitions())

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists Hr;

# COMMAND ----------

emp_df.write.format("parquet").mode("overwrite").saveAsTable("Hr.Employees")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe table extended hr.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) as cnt from hr.employees;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC employee_id as emp_id,
# MAGIC first_name as f_name,
# MAGIC last_name as l_name,
# MAGIC email as email_id,
# MAGIC phone_number as contact,
# MAGIC hire_date as hire_dt,
# MAGIC job_id as job_id,
# MAGIC salary,
# MAGIC commission_pct as comm,
# MAGIC manager_id as mgr_id,
# MAGIC department_id as dept_id,
# MAGIC load_ts
# MAGIC from hr.employees where manager_id is not null;

# COMMAND ----------

driver = "org.postgresql.Driver"
urls = "jdbc:postgresql://postgre.cm9oj1260goc.ap-south-1.rds.amazonaws.com/"
table = "kjjkyuyuuyiuyuy"
user = "postgres"
password = "Spark123"
emp_df.write.format("jdbc").option("driver", driver).option("url",urls).option("dbtable", table).option("mode", "overwrite").option("user",user).option("password", password).save()
