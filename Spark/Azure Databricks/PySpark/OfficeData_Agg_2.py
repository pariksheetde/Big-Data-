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

# calculate total number of students enrolled in each course
agg_cnt_per_course = student_df.groupBy(col("course")) \
.agg(
  count(col("*")).alias("cnt_student_per_course")
  ) \
.orderBy(col("cnt_student_per_course").desc())
agg_cnt_per_course.show()

# COMMAND ----------

# calculate total number of male and female students in each course, gender
agg_cnt_course_per_gender_df = student_df.groupBy(col("gender"), col("course")) \
.agg(
  count(col("*")).alias("cnt_of_students_per_gender")
  )
agg_cnt_course_per_gender_df.show()

# COMMAND ----------

# calculate total number of male and female students in each course, gender
agg_cnt_course_per_gender_df = student_df.groupBy(col("gender"), col("course")) \
.agg(
  sum(col("marks")).alias("total_marks_per_gender")
  )
agg_cnt_course_per_gender_df.show()

# COMMAND ----------

# calculate min, max, avg marks each course by each age group
agg_cnt_course_per_gender_df = student_df.groupBy(col("course"), col("age")) \
.agg(
  min(col("marks")).alias("min_marks"),
  max(col("marks")).alias("max_marks"),
  avg(col("marks")).alias("avg_marks")
  )
agg_cnt_course_per_gender_df.show()
