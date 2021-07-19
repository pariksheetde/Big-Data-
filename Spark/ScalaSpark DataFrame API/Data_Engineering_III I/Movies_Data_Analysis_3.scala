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

object Movies_Data_Analysis_3 extends App {
  println("Movies Data Analysis 3")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 3")
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

  //  calculate Ratings for the movies
  val ratings_df_cln_1 = movies_df.select("Title", "Director", "IMDB_Rating", "Rotten_Tomatoes_Rating", "Major_Genre")
    .withColumn("RT_Rating", col("Rotten_Tomatoes_Rating") / 10)
    .drop("Rotten_Tomatoes_Rating")

  val ratings_df_cln_2 = ratings_df_cln_1.select(col("Title"),
    col("Director"), col("IMDB_Rating"), col("RT_Rating"),
    col("Major_Genre"),
    coalesce(col("IMDB_Rating"), col("RT_Rating")).alias("Rating"))

  val ratings_df = ratings_df_cln_2.na.fill(Map(
    "Director" -> "No Director",
    "Rating" -> 0,
    "Major_Genre" -> "Not Available"
  )).sort(col("Rating").desc_nulls_last)
    .show(false)

  spark.stop()
}