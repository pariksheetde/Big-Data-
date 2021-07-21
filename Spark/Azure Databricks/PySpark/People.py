# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

people_schema = StructType([StructField('age', IntegerType(), True),
                          StructField('name', StringType(), True)
                         ])    


people_df = spark.read.json("/FileStore/tables/people.json", schema = people_schema)
people_df.printSchema()
people_df.show(truncate = False)

# COMMAND ----------

# filter out null age
fil_people_df = people_df.filter("age is not null")
fil_people_df.show()

# COMMAND ----------

age_df = people_df.withColumn('new_age', col('age') *2)
age_df.show()
age_df.printSchema()
