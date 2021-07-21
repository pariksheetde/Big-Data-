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
    print("Accessing Customers file")
    
cust_schema = StructType([StructField('Cust_Key', IntegerType(), True),
                          StructField('Cust_ID', IntegerType(), True),
                          StructField('F_Name', StringType(), True),
                          StructField('L_Name', StringType(), True),
                          StructField('Country', StringType(), True),
                          StructField('CTC', IntegerType(), True),
                          StructField('Baic_Pay', DoubleType(), True),
                          StructField('ZIP', IntegerType(), True),
                          StructField('Commission', DoubleType(), True)
                         ])    

cust_df = spark.read.csv("/FileStore/tables/Customers-2.csv", header = True, schema = cust_schema)
cust_df_high_ctc = cust_df.filter(col("CTC") >= 2500000)
cust_df.show()

# COMMAND ----------

# filter out the records where country = England and CTC >= 2000000
cust_coun_df = cust_df.where("Country = 'England' and CTC >= 2000000")
cust_coun_df.show(truncate = False)

# COMMAND ----------

# select the countries where values are Spain and England
countries = ["Spain", "France"]
cust_frn_spn = cust_df.filter(cust_df.Country.isin(countries))
cust_frn_spn.show()

# COMMAND ----------

# select the countries where values are not Spain and England
countries = ["Spain", "England"]
coun_not_spn_eng = cust_df.filter(~cust_df.Country.isin(countries))
coun_not_spn_eng.show()

# COMMAND ----------

# # select the countries where values are not Spain and England
coun_spn_frn_low_inc = cust_df.filter("Country in ('Spain', 'England') and CTC <= 1500000")
coun_spn_frn_low_inc.show()

# COMMAND ----------


