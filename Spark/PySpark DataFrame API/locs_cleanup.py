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

# locs = spark.read.csv(r"D:\Code\DataSet\SparkDataSet\locs_cleanup.csv", header=True).show(10)

# read the datafile from the location
locations = (spark.read.option("header", "True")
         .option("schema", True)
         .option("dateFormat", "dd-mm-yyyy")
         .option("timestampFormat", "dd-mm-yyyy hh24:mm:ss")
         .csv(r"D:\Code\DataSet\SparkDataSet\locs_cleanup.csv")
         .select("_c0", "location_id", "street_address", "postal_code", "city", "state_province", "country_id")
         )
# locations.show(5)
loc_rename = locations.withColumnRenamed("_c0", "loc_id").withColumnRenamed("location_id", "address").withColumnRenamed("street_address", "code") \
                      .withColumnRenamed("postal_code", "native").withColumnRenamed("city", "state").withColumnRenamed("state_province", "country") \
                      .select("loc_id", "address", "code", "native", "state", "country")

loc_rename.show(15, truncate=False)
