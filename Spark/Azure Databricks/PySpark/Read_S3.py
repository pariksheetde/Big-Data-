# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

aws_keys_df = spark.read.csv("/FileStore/tables/aws_keys.csv", header = True)

# COMMAND ----------

import pyspark.sql.functions
import urllib
ACCESS_KEY = aws_keys_df.where(col("user") == "databricks").select("access_key").collect()[0].access_key
SECRET_KEY = aws_keys_df.where(col("user") == "databricks").select("secret_key").collect()[0].secret_key
ENCODE_SECRET_KEY = urllib.parse.quote(SECRET_KEY,"")

# COMMAND ----------

AWS_S3_BUCKET = "hr-data-lake"
MOUNT_NAME = "/mnt/hr-data-lake"
SOUURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODE_SECRET_KEY, AWS_S3_BUCKET)
# dbutils.fs.mount(SOUURCE_URL, MOUNT_NAME)

# COMMAND ----------

# MAGIC %fs ls "/mnt/hr-data-lake/employees.csv"

# COMMAND ----------

emp_df = spark.read.csv("/mnt/hr-data-lake/employees.csv", header = True)
emp_salary_df = emp_df.select("*") \
.filter(col("Salary") >= 2500000)
emp_salary_df.show()

# COMMAND ----------

emp_salary_df.write.format("csv").mode("overwrite").save("/mnt/hr-data-lake/high_paid_emp", header = True)

# COMMAND ----------

high_paid_res_df = spark.read.csv("/mnt/hr-data-lake/high_paid_emp", header = True)
high_paid_res_df.show()
print("Records Effected ", high_paid_res_df.count())
