package Data_Engineering_3

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object Cars_Data_Analysis_1 extends App {
  println("Cars Data Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Cars Data Analysis 1")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  //  read the movies.json datafile from the location
  val cars_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .json("D:/DataSet/DataSet/SparkDataSet/cars.json")

  cars_df.show(false)

  //  count number of records in the cars_df DF
  println(s"Number of records in the cars_df DF: ${cars_df.count()}")

  //  number of origin
  import spark.implicits._
  val origin_cnt = cars_df.select(col("Origin"))
    .groupBy(col("Origin"))
    .agg(
      count($"Origin")
    ).show(false)

  //  compute year, month, dd
  val purchase_df = cars_df.select($"Origin", col("Name"), 'Weight_in_lbs, 'Year)
    .withColumn("Purchasing_Year",substring(col("Year"), 1, 4))
    .withColumn("Purchasing_Month", substring(col("Year"),6,2))
    .withColumn("Purchasing_Date", substring(col("Year"),9,2))
    .show(false)

  spark.stop()

}