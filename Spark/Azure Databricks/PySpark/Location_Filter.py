# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

if __name__ == "__main__":
    print("Accessing MySQL DB")
    
jdbcDict= {'hostname':'demetadata.mysql.database.azure.com','location_name':'hr','account_name':'DEadmin@demetadata','account_key':'Tredence@123'}
Qry_1 = 'select * from locations'

connectionUrl = "jdbc:mysql://{0}/{1}?useUnicode=true&useJDBCCompliantTimezoneShift=true&useLegacyDatetimeCode=false&serverTimezone=UTC" \
.format(jdbcDict['hostname'], jdbcDict['location_name'])
connectionProp = {
"user" : jdbcDict['account_name'],
"password" : jdbcDict['account_key'],
"driver" : "com.mysql.jdbc.Driver"
}
pdq_1 = "({0}) pdq".format(Qry_1)
loc_df = spark.read.jdbc(url=connectionUrl, table = pdq_1 , properties = connectionProp)
loc_sel = loc_df.selectExpr("location_id as LocID", "location_name as Loc_Name") \
.where("Loc_Name <> 'London'") \
.show(10, False)
