package Data_Engineering_IV

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

object IPL_Analysis_1 {
  println("IPL Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "IPL Analysis 1")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  def read_IPL(filename: String) = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv(s"D:/Code/DataSet/SparkDataSet/$filename")

  import spark.implicits._
  val ipl_df = read_IPL("IndianPremierLeague.csv")

  ipl_df.printSchema()
  ipl_df.show(false)

//  count number of IPL matches played
  val matches_played_cnt = println(s"Number of IPL matches played: ${ipl_df.count()}") // 816

//  count number of matches played in which city and stadium
  println(s"count number of matches played in which city and stadium")
  import spark.implicits._
  val city_venue_cnt = ipl_df.select($"city", $"venue")
    .groupBy($"city", $"venue")
    .agg(
      count($"city").as("City_Cnt")
    ).sort($"City_Cnt".desc)
    .show(false)

  def main(args: Array[String]): Unit = {

//  which team has won most number of games
    import spark.implicits._
    println(s"Which team has won most number of games")
    val max_win_cnt = ipl_df.select($"winner")
      .groupBy($"winner")
      .agg(
        count($"winner").alias("maximum_win")
      ).sort($"maximum_win".desc)
      .show(false)

    spark.stop()
  }
}
