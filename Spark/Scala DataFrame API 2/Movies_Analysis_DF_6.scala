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

object Movies_Analysis_DF_6 extends App {
  println("Movies Data Analysis 6")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 6")
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

//  select Title, Director, Top Rating for each Genre
  val cln_movies_df = movies_df.na.fill(Map(
    "Director" -> "No Director",
    "Major_Genre" -> "Not Listed"
  ))
  val top_rating = cln_movies_df.select(col("Title"), col("Director"), col("Major_Genre"),
    col("Rotten_Tomatoes_Rating"), col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating") / 10, col("IMDB_Rating")).as("Rating")
  )

// calculate top rating for each director in each genre
  println(s"Calculate max, min, avg Rating for each Director and Genre")
  val top_rating_genre = top_rating.select(col("Director"), col("Major_Genre"), col("Rating"))
    .groupBy(col("Director"), col("Major_Genre"))
    .agg(
      max(col("Rating")).as("Max_Rating"),
      min(col("Rating")).alias("Min_Rating"),
      round(avg(col("Rating")),2).as("Avg_Rating")
    )
    .sort(col("Director").desc_nulls_last)
//    .where("Director in ('Steven Spielberg')")
    .show(false)

    spark.stop()
}
