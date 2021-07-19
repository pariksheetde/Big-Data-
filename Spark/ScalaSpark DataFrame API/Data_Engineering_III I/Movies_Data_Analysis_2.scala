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

object Movies_Data_Analysis_2 extends App {
  println("Movies Data Analysis 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 2")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  //  read the movies.json datafile from the location
  val movies_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json")

  movies_df.printSchema()
  movies_df.show(false)

  //  select only the columns that are required
  val movies_df_1 = movies_df.select(col("Title"), col("Director"), col("IMDB_Rating"),
    col("Major_Genre"), col("Production_Budget"), col("Release_Date"), col("Rotten_Tomatoes_Rating"),
    col("US_DVD_Sales"), col("US_Gross"))
  //      .show(false)

  //  convert NULL for US_DVD_Sales & US_Gross to 0 and Director
  val profit_cln = movies_df_1.na.fill(Map(
    "US_DVD_Sales" -> 0,
    "US_Gross" -> 0,
    "Director" -> "Not Listed"
  )).drop("IMDB_Rating", "Major_Genre", "Release_Date", "Rotten_Tomatoes_Rating") // drop unwanted columns
  //    .show(false)

  //  calculate gross_collection, profit for each movie
  println(s"Calculate the profit for each movie")
  val profit_df = profit_cln.select(col("Title"), col("Director"), col("Production_Budget"),
    col("US_DVD_Sales"), col("US_Gross"),
    (col("US_DVD_Sales") + col("US_Gross")).alias("Gross_Collection"))
    .withColumn("Profit", col("Gross_Collection") - col("Production_Budget"))
    .show(false)

  spark.stop()
}