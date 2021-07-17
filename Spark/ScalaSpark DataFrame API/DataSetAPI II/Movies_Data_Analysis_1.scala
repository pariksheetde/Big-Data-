package DataSetAPI

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object Movies_Data_Analysis_1 {
  println("Movies Data Analysis 1")

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Movies Data Analysis 1")
    .getOrCreate()

//  define case class for movie
  case class Ratings(userid: Int, movieid: Int, rating: Int, ins_dt: Long)

//  define schema for movies.csv
  val movies_csv_schema = new StructType()
    .add("userid", IntegerType)
    .add("movieid", IntegerType)
    .add("rating", IntegerType)
    .add("ins_dt", LongType)


def compute() = {
  import spark.implicits._
  val movies_df = spark.read
    .format("csv")
    .schema(movies_csv_schema)
    .option("header", true)
    .load("D:/Code/DataSet/SparkStreamingDataSet/movies/ratings.csv")
    .as[Ratings]

  val top_movies_df = movies_df.groupBy("movieid")
    .agg(
      max("rating").as("highest_rating"),
      count("rating").alias("cnt_rating")
    ).orderBy(desc("cnt_rating"))

  val top_movies = top_movies_df.show(top_movies_df.count().toInt, false)

}
  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
