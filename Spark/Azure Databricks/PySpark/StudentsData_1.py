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

total_marks_df = student_df.withColumn("totalmarks", lit(120))
total_marks_df.show(truncate = False)
print("Records Effected: ", total_marks_df.count())

# COMMAND ----------

# calculate the average marks of each student
avg_marks_df = total_marks_df.withColumn("avg_marks", (col("marks") / col("totalmarks")) * 100)
avg_marks_df.show(truncate = False)

# COMMAND ----------

oop_df = avg_marks_df.select("*") \
.filter((student_df.course == 'OOP') & (total_marks_df.marks > 80))
oop_df.show(truncate = False)
print("Number of cloud students", oop_df.count())

# COMMAND ----------

cloud_df = avg_marks_df.select("*") \
.where((student_df.course == 'Cloud') & (avg_marks_df.marks > 60))
cloud_df.show(truncate = False)
print("Number of cloud students", cloud_df.count())

# COMMAND ----------

all_oop_cloud_df = cloud_df.union(oop_df) \
.select(col("name"), col("course") ,col("avg_marks"))
all_oop_cloud_df.show(150,truncate = False)
print("Number of cloud students", all_oop_cloud_df.count())
