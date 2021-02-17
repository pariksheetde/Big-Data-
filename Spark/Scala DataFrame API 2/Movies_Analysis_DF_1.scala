package Data_Engineering_III

import Data_Engineering_I.Dept_Emp_Analysis_3.{Dept_ID_Renamed, dept_df, emp_df}
import Data_Engineering_I.Dept_Emp_Analysis_4.spark
import Data_Engineering_I.Dept_Emp_Analysis_5.spark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._

object Movies_Analysis_DF_1 extends App {
  println("Movies Data Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 1")
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

//  select only the columns that are required
  val movies_df_1 = movies_df.select(col("Title"), col("Director"), col("IMDB_Rating"),
    col("Major_Genre"), col("Production_Budget"), col("Release_Date"), col("Rotten_Tomatoes_Rating"),
    col("US_DVD_Sales"), col("US_Gross"))

//  convert NULL to value for Major_Genre, Director
  val null_convert = movies_df_1.na.fill(Map(
    "Major_Genre" -> "Not Listed",
    "Director" -> "Not Listed"
  ))

//  count the number of movies in each genre and sort Cnt_Genre in desc order
  println(s"Count of Movies / Title in each Genre")
  val genre_cnt = null_convert.groupBy("Major_Genre")
    .agg(
      count("Major_Genre").as("Cnt_Genre")
    )
    .sort(col("Cnt_Genre").desc)
    .show(false)

//  calculate total earning for each movie
  val profit = null_convert.select(col("Title"), col("Major_Genre"), col("IMDB_Rating"),
    col("Production_Budget"), col("Rotten_Tomatoes_Rating"),
    col("US_DVD_Sales"), col("US_Gross"),
    (col("US_DVD_Sales") + col("US_Gross")).as("Total"))
    .show(false)

  spark.stop()
}
