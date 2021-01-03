import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.types import DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.functions import spark_partition_id

if __name__ == "__main__":
    print("Employee Details")

# set the SparkSession
spark = SparkSession.builder.appName("Employee Details").master("local[3]").getOrCreate()

# define schema for games location.csv
location_schema = StructType(
                          [
                           StructField("location_id", IntegerType(), False),
                           StructField("street_address", StringType(), False),
                           StructField("postal_code", IntegerType(), False),
                           StructField("city", StringType(), False),
                           StructField("state_province", StringType(), False),
                           StructField("country_id", StringType(), False)
                           ]
                         )

# read the datafile from the location
locations = (spark.read.option("header", "True")
         .option("schema", location_schema)
         .option("dateFormat", "dd-mm-yyyy")
         .option("timestampFormat", "dd-mm-yyyy hh24:mm:ss")
         .csv(r"D:\Code\DataSet\SparkDataSet\locations.csv")
         .select("location_id", "street_address", "postal_code", "city", "state_province", "country_id")
         )
locations.show(3, truncate=False)

# define schema for games location.csv
departments_schema = StructType(
                          [
                           StructField("department_id", IntegerType(), False),
                           StructField("department_name", StringType(), False),
                           StructField("manager_id", IntegerType(), False),
                           StructField("location_id", IntegerType(), False)
                           ]
                         )

# read the datafile from the departments
departments = (spark.read.option("header", "True")
         .option("schema", departments_schema)
         .option("dateFormat", "dd-mm-yyyy")
         .option("timestampFormat", "dd-mm-yyyy hh24:mm:ss")
         .csv(r"D:\Code\DataSet\SparkDataSet\departments.csv")
         .select("department_id", "department_name", "manager_id", "location_id")
         )
departments.show(3, truncate=False)

# join locations and departments
join_loc_dept = locations.join(departments, on="location_id", how="inner") \
                         .select("location_id", "department_id", "department_name")

join_loc_dept.show(10)
print(f'Rows effected: {join_loc_dept.count()}')