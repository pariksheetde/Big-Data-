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

# count of students in each course
agg_course_cnt_df = student_df.groupBy(col("course")) \
.agg(count(col("name")).alias("student_cnt_per_course")) \
.orderBy(col("student_cnt_per_course").desc())
agg_course_cnt_df.show()

# COMMAND ----------

# count of students in each gender
agg_gender_cnt_df = student_df.groupBy(col("course")) \
.agg(count(col("gender")).alias("gender_cnt_per_course")) \
.orderBy(col("gender_cnt_per_course").desc())
agg_gender_cnt_df.show()

# COMMAND ----------

# calculate the sum of marks for each gender
agg_marks_sum_df = student_df.groupBy(col("gender")) \
.agg(sum(col("marks")).alias("total_marks_per_gender")) \
.sort(col("total_marks_per_gender").asc())
agg_marks_sum_df.show()

# COMMAND ----------

# calculate the number of students for each gender
agg_students_cnt_df = student_df.groupBy(col("gender")) \
.agg(count(col("name")).alias("student_cnt_per_gender")) \
.orderBy(col("student_cnt_per_gender").desc())
agg_students_cnt_df.show()
