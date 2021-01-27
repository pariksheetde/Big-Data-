package BigData

import BigData.cars_schema_1.{cars_schema, spark}
import BigData.cars_schema_2.{data, schema, spark}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object cars_DF extends App {

//  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("USA Cars Details using StructType")
    .getOrCreate()

  // define schema for cars DF
  val cars_schema = StructType(List(
    StructField("ID", IntegerType),
    StructField("price", IntegerType),
    StructField("brand", StringType),
    StructField("model", StringType),
    StructField("year", IntegerType),
    StructField("title_status", StringType),
    StructField("mileage", StringType),
    StructField("color", StringType),
    StructField("vin", StringType),
    StructField("lot", IntegerType),
    StructField("state", StringType),
    StructField("country", StringType),
    StructField("condition", StringType)
  ))


//  read the csv file into DF
  val cars_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(cars_schema) // enforces the user defined schema
    .option("mode", "failFast") // dropMalFormed, permissive
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate
    .option("dateFormat", "YYYY-MM-dd")
    .option("path","D:/Code/DataSet/SparkDataSet/cars_USA.csv")
    .load()

//  select the columns that are required
  val cars = cars_df.selectExpr("ID", "price as Price", "brand as Brand", "year as YYYY", "title_status as Title_Status", "mileage as Mileage",
  "color as Color", "lot as Lot", "state as State", "country as Country", "condition as Condition")

//  write the DF to the nes file
  cars.write
    .format("json")
    .mode(SaveMode.Overwrite) // append, ignore, errorIfExists
    .option("header", "true")
    .option("path", "H:/ScalaSparkOutput/Cars/")
    .save()

//  read the DF which was saved in previous step
  val cars_final_df = spark.read
    .format("json")
    .option("header", "true")
    .option("mode", "failFast") // dropMalFormed, permissive
    .option("dateFormat", "YYYY-MM-dd")
    .option("path","H:/ScalaSparkOutput/Cars/part*")
    .load()
    .show()

  spark.stop()
}
