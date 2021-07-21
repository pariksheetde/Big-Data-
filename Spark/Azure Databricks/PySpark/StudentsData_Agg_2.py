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
    print("Accessing Students REcords")
    
student_schema = StructType([StructField('age', IntegerType(), True),
                          StructField('gender', StringType(), True),
                          StructField('name', StringType(), True),
                          StructField('course', StringType(), True),
                          StructField('roll', StringType(), True),
                          StructField('marks', IntegerType(), True),
                          StructField('email', StringType(), True)
                         ])    

student_df = spark.read.csv("/FileStore/tables/StudentData.csv", header = True, schema = student_schema)
student_df.printSchema()
student_df.show(truncate = False)

# COMMAND ----------

# calculate the number of students for each gender, course
agg_std_cnt_df = student_df.groupBy(col("gender"), col("course")) \
.agg(count(col("name")).alias("cnt_std_per_gender_course")) \
.sort(col("gender").desc(), col("course").asc())
agg_std_cnt_df.show()

# COMMAND ----------

# calculate the marks, number of students for each gender, course
agg_std_cnt_df = student_df.groupBy(col("gender"), col("course")) \
.agg(count(col("name")).alias("cnt_std_per_gender_course")) \
.sort(col("gender").desc(), col("course").asc())
agg_std_cnt_df.show()

# COMMAND ----------

# calculate the number of students for each gender, course
agg_std_cnt_df = student_df.groupBy(col("gender"), col("course")) \
.agg(
  count(col("name")).alias("cnt_std_per_gender_course"),
  sum(col("marks")).alias("sum_marks")
) \
.sort(col("gender").desc(), col("course").asc())
agg_std_cnt_df.show()

# COMMAND ----------


