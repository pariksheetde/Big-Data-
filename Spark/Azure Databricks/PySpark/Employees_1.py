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

# set the SparkSession
emp_csv = SparkSession.builder.appName("Accessing Employees file").master("local[3]").getOrCreate()
emp_df = emp_csv.read.csv("/FileStore/tables/employees.csv") \
.withColumnRenamed("_c0", "Emp_ID") \
.withColumnRenamed("_c1", "F_Name") \
.withColumnRenamed("_c2", "L_Name") \
.withColumnRenamed("_c3", "Manager_ID") \
.withColumnRenamed("_c4", "DeptID") \
.withColumnRenamed("_c5", "Salary") \
.show(10, False)

# print("Number of Partitions ", emp_df.rdd.getNumPartitions())
