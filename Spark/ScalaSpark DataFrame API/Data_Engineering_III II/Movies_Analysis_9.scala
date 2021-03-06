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
import org.apache.spark.sql.expressions.Window

object Movies_Analysis_9 {
  println("Movies Data Analysis 9")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 9")
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

  //  calculate profit, rating earned for each director for each Genre
  val profit_null_df = movies_df.na.fill(Map(
    "Director" -> "Not Listed",
    "Major_Genre" -> "Not Listed",
    "US_DVD_Sales" -> 0,
    "US_Gross" -> 0,
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 0
  ))
  profit_null_df.show(false)


  def compute() = {
    //  select only the required columns
    val sel_movies_df = profit_null_df.select(col("Major_Genre"), col("Title"), col("Director"),
      (coalesce(col("IMDB_Rating"), col("Rotten_Tomatoes_Rating") / 10)).as("Rating"))
      .where("Rating != 0")
    sel_movies_df.show(false)

    //  define window aggregates partitioned by Major_Genre and sort on Rating in ascending order
    val winSpec = Window.partitionBy("Major_Genre").orderBy(col("Rating").asc)

    //  specify the rank() logic to select only the lowest ranking where rank = 1
    val rank_df = sel_movies_df.withColumn("rank", rank().over(winSpec)).where("rank = 1")
    rank_df.show(false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
