package Data_Engineering_III

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._

object Movies_Analysis_DF_4 extends App {
  println("Movies Data Analysis 4")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 4")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    //    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
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
    .json("D:/Code/DataSet/SparkDataSet/movies.json")

  movies_df.printSchema()
  movies_df.show(false)

//  count number of directors
  println(s"Count Number of directors")
  val cnt_directors = movies_df.select(col("Director"))
    .agg(
      countDistinct("Director").alias("Cnt_Directors")
    ).show(false)

//  convert NULL to values
  val top_movies_cln = movies_df.na.fill(Map(
    "Director" -> "No Director",
    "US_DVD_Sales" -> 0,
    "US_Gross" -> 0
  ))

//  calculate the sum of US_DVD_Sales + US_Gross
  println(s"calculate the sum of US_DVD_Sales + US_Gross for each movie")
  val profit = top_movies_cln.select(col("Title"), col("Director"),
    col("US_DVD_Sales"), col("US_Gross"),
    (col("US_DVD_Sales") + col("US_Gross")).alias("Profit")
  )
  profit.show(false)

//  count number of movies by each director
  println(s"calculate the sum of collections for each director and number of movies")
  val cnt_movies_per_dir = profit.select(col("Director"), col("Title"), col("Profit"))
    .groupBy("Director")
    .agg(
      count(col("Title")).alias("Number_of_Movies"),
      sum(col("Profit")).as("Collection")
    )
    .sort(col("Number_of_Movies").desc_nulls_last)
    .show(false)

  spark.stop()
}
