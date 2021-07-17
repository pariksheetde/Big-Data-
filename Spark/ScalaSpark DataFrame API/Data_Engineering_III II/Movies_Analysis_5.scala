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

object Movies_Analysis_5 extends App {
  println("Movies Data Analysis 5")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 5")
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
    .json("D:/Code/DataSet/SparkDataSet/movies.json")

  movies_df.printSchema()
  movies_df.show(false)

//  select Title, Director, Rating
//  convert NULL into a value
  val cln_movies_df = movies_df.na.fill(Map(
    "Director" -> "No Director",
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 0
  ))


  //  count number of movies for each director in each Genre
  println(s"Count Number of movies for each director in each Genre")
  val cnt_directors = movies_df.select(col("Director"), col("Major_Genre"))
    .groupBy(col("Director"),col("Major_Genre"))
    .agg(
      count("Major_Genre").alias("Cnt_of_Movies_Genre")
    ).sort(col("Director").asc_nulls_last)
//    .where(col("Director") === "Steven Spielberg") // 23
//    .where(col("Director") === "Martin Scorsese") // 15
//    .where("Director in ('Steven Spielberg', 'Martin Scorsese')") // (23 + 15) = 38
  cnt_directors.show(false)

  spark.stop()

}
