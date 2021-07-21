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
    print("Accessing Students Records")
    
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
print(f"Number of partition: {student_df.rdd.getNumPartitions()}")

# COMMAND ----------

spark_SQL = student_df.createOrReplaceTempView("students")
spark_qry = spark.sql("select * from students")
spark_qry.show(truncate = False)

# COMMAND ----------

# calculate max, avg, min of marks for each course, gender
agg_std_marks_per_course = spark.sql("select \
                                     course, gender, \
                                     max(marks) as max_marks, \
                                     avg(marks) as avg_marks, \
                                     min(marks) as min_marks \
                                     from students \
                                     group by course, gender \
                                     order by course")
agg_std_marks_per_course.show(truncate = False)
print(f"Number of partition: {agg_std_marks_per_course.rdd.getNumPartitions()}")

# COMMAND ----------

# write the result of the agg_std_marks_per_course DF to the table
spark.conf.set("spark.sql.shuffle.partitions", 1)
agg_std_marks_per_course.write.mode("Overwrite").options(header = "True").csv("/FileStore/tables/output/Student_Marks")

# COMMAND ----------

# select the data which was previously saved inside the table
std_marks_schema = StructType([StructField('course', StringType(), True),
                          StructField('gender', StringType(), True),
                          StructField('max_marks', DoubleType(), True),
                          StructField('avg_marks', DoubleType(), True),
                          StructField('min_marks', DoubleType(), True)                         
                         ])  
std_marks_df = spark.read.csv("/FileStore/tables/output/Student_Marks", header = True, schema = std_marks_schema)
std_marks_df.printSchema()
std_marks_df.show(truncate = False)
print("Records Effected: ", std_marks_df.count())
print(f"Number of partition: {std_marks_df.rdd.getNumPartitions()}")

# COMMAND ----------

# drop the unwanted directories and files
# dbutils.fs.rm('/FileStore/tables/output/Student_Marks',True)
