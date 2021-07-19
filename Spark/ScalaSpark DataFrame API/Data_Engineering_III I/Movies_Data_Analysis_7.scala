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

object Movies_Data_Analysis_7 {
  println("Movies Data Analysis 7")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 7")
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
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json")

  movies_df.printSchema()
  movies_df.show(false)

  def compute() = {
  //  calculate profit, rating earned for each director for each Genre
  val profit_null_df = movies_df.na.fill(Map(
    "Director" -> "Not Listed",
    "Major_Genre" -> "Not Listed",
    "US_DVD_Sales" -> 0,
    "US_Gross" -> 0
  ))

  val profit_rating_cln_df = profit_null_df.select(col("Director"), col("Major_Genre"),
    col("US_DVD_Sales"), col("US_Gross"), col("Title"),
    col("IMDB_Rating"), col("Rotten_Tomatoes_Rating"),
    coalesce(col("IMDB_Rating"), col("Rotten_Tomatoes_Rating") / 10).as("Rating"),
    (col("US_DVD_Sales") + col("US_Gross")).as("Profit")
  )
  profit_rating_cln_df.show(false)

    println(s"Calculate Total Gross, Number of Movies and Avg(Rating) for each Director, Genre")
    val profit_rating_agg = profit_rating_cln_df.select(col("Director"), col("Major_Genre"),
      col("US_Gross"), col("Rating"), col("Title"), col("Profit"))
      .groupBy(col("Director"), col("Major_Genre"))
      .agg(
        sum("Profit").as("Sum_Gross"),
        count("Title").as("Number_of_Movies"),
        round(avg("Rating"), 2).alias("Avg_Rating")
      )
      .sort(col("Director").asc_nulls_last)
      .where("Director = 'Steven Spielberg'")
      .show(false)

  }

    def main(args: Array[String]): Unit = {
      compute()
      spark.stop()
    }
}