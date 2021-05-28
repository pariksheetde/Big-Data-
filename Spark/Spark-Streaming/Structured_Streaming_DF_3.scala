package Spark_Structured_Streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
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
import org.apache.spark.sql.streaming.Trigger

object Structured_Streaming_DF_3 {
  println("Spark Structured Streaming DF 3")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .appName("Spark Structured Streaming DF 3")
    .getOrCreate()

  //  read the data for bands
  val bands_df = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/bands")

  //  read the data for bands
  val guitars_df = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/guitars")

  //  read the data for bands
  val guitarPlayers_df = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/guitarPlayers")

  //  define the schema for guitars
  val bands_schema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("hometown", StringType),
    StructField("year", IntegerType)
  ))

  //  join the DF between guitarPlayers and bands DF
  val first_joincond = bands_df.col("id") === guitarPlayers_df.col("band")
  val guitar_players_bands_df = guitarPlayers_df.join(bands_df, first_joincond, "inner")
    .drop(bands_df.col("id"))

  def joinstreamwithstatic() = {
    val streamed_bands_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load() // DF with a single column called value
      .select(from_json(col("value"), bands_schema).as("bands"))
      .selectExpr("bands.id as id", "bands.name as name", "bands.hometown as hometown", "bands.year as year")

    val second_joincond = guitarPlayers_df.col("band") === streamed_bands_df.col("id")
    val streamed_band_guitarist = streamed_bands_df.join(guitarPlayers_df, second_joincond, "inner")
    streamed_band_guitarist.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    joinstreamwithstatic()
  }
}
