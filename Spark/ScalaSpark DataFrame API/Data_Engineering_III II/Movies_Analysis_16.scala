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
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object Movies_Analysis_16 {
  println("Movies Data Analysis 16")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 16")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    //    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  //  read the movies.json datafile from the location
  val movies_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .json("D:/Code/DataSet/SparkDataSet/movies.json")

  println(s"Number of Records: ${movies_df.count()}")
  movies_df.show(false)

  //  convert NULL to values for US_DVD_Sales and US_Gross
  val null_chk = movies_df.na.fill(Map(
    "US_Gross" -> 0,
    "US_DVD_Sales" -> 0
  ))

  def compute() = {
    //  best movies based on collection
    val movies_coll_df = null_chk.select(col("Title"), col("US_DVD_Sales"), col("US_Gross"),
      (col("US_DVD_Sales") + col("US_Gross")).as("Collection"))

    import spark.implicits._
    println(s"Best Movies - Collection")
    val best_movies_df = movies_coll_df.select("Title", "Collection")
      .withColumn("Rank", rank().over(Window.orderBy($"Collection".desc)))
    //    .show(false)

    //  worst movies based on collection
    println(s"Worst Movies - Collection")
    import spark.implicits._
    val worst_movies_df = movies_coll_df.select("Title", "Collection")
      .filter($"Collection" =!= 0)
      .withColumn("Rank", rank().over(Window.orderBy($"Collection".asc)))
    //    .show(false)

    //  worst movies based on collection
    println(s"Best &  Worst Movie - Collection")
    val best_worst_movie = best_movies_df.union(worst_movies_df)
      .where("Rank = 1").show(false)

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
