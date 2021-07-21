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

# drop the duplicate records for gender, course, age
unique_df = student_df.select(col("gender"),col("course"),col("age")).dropDuplicates() \
.sort("gender")
unique_df.show(truncate = False)
unique_df.count()

# COMMAND ----------

# drop the duplicate records for gender, course, age and select all the columns
all_unique_df = student_df.dropDuplicates(["gender","course","age"])
all_unique_df.show(truncate = False)
print("Records Effected: ", all_unique_df.count())

# COMMAND ----------

# drop the duplicate records for gender, course, age and sort by marks by asc order
sort_df_1 = student_df.dropDuplicates(["gender","course","age"]) \
.sort(col("marks").asc())
sort_df_1.show(truncate = False)
print("Records Effected: ", sort_df_1.count())

# COMMAND ----------

# drop the duplicate records for gender, course, age and sort by marks by desc order
sort_df_2 = student_df.dropDuplicates(["gender","course","age"]) \
.sort(col("marks").desc())
sort_df_2.show(truncate = False)
print("Records Effected: ", sort_df_2.count())

# COMMAND ----------

# drop the duplicate records for gender, course, age and sort by age asc, marks asc order
sort_df_3 = student_df.dropDuplicates(["gender","course","age"]) \
.sort(col("age").asc(), col("marks").asc())
sort_df_3.show(truncate = False)
print("Records Effected: ", sort_df_3.count())

# COMMAND ----------

# drop the duplicate records for gender, course, age and sort by age asc, marks desc order
sort_df_4 = student_df.dropDuplicates(["gender","course","age"]) \
.sort(col("age").asc(), col("marks").desc())
sort_df_4.show(truncate = False)
print("Records Effected: ", sort_df_4.count())

# COMMAND ----------

# drop the duplicate records for gender, course, age and sort by age asc, marks asc order
sort_df_5 = student_df.dropDuplicates(["gender","course","age"]) \
.orderBy(col("age").asc(), col("marks").asc())
sort_df_5.show(truncate = False)
print("Records Effected: ", sort_df_5.count())

# COMMAND ----------

# drop the duplicate records for gender, course, age and sort by age asc, marks asc order
sort_df_6 = student_df.dropDuplicates(["gender","course","age"]) \
.orderBy(col("age").asc(), col("marks").desc())
sort_df_6.show(truncate = False)
print("Records Effected: ", sort_df_6.count())
