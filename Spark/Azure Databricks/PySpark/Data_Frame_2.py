# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType, DateType
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Data Frame 2").getOrCreate()

data = [("Samsung", "Galaxy S8", "Android" ,65000, "15-10-2021"),
       ("Apple", "IPhone 10 MAX", "iOS", 75000, "12-11-2020"),
        ("Apple", "IPhone X", "iOS", 125000, "12-10-2017"),
        ("Redmi", "Redmi 9", "Android", 10900,"12-12-2015")
       ]

schema = StructType([ \
    StructField("Company",StringType(),True), \
    StructField("Model",StringType(),True), \
    StructField("OS",StringType(),True), \
    StructField("Price", StringType(), True), \
    StructField("Launch_Dt", StringType(), True)
  ])

df = spark.createDataFrame(data=data,schema=schema)

clean_df = df.withColumn("Date", substring("Launch_Dt", 1, 2)) \
.withColumn("Month", substring("Launch_Dt", 4,2)) \
.withColumn("Year", substring("Launch_Dt", 7,4))

final_df = clean_df.select(col("Company"),col("Model"), col("OS"), col("Price"),col("Date").cast("int").alias("Date"), col("Month").cast("int").alias("Month"),
                          col("Year").cast("int").alias("Year"))

    
    
final_df.printSchema()
final_df.show(100)
