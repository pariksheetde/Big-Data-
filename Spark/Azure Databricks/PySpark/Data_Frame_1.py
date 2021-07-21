# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Data Frame 1").getOrCreate()

dataset = [("Monica", "Bellucci", 3642019), ("Pamela", "Parker", 2250000),
("Kate", "Beckinsale", 1700000),
("Audry", "Hepburn", 1750000),
("Jennifer", "Lawrence", 1957000),
("Carmen", "Electra", 1550000),
("Jennifer", "Garner", 3225000),
("Michelle", "Obama", 1400500),
("Kirsten", "Steward", 2150000),
("Sophia", "Turner", 2945000)]

df = spark.createDataFrame(dataset).toDF("F_Name", "L_Name", "Salary")
sel_df = df.where("Salary >= 3000000")

sel_df.printSchema()
sel_df.show()
