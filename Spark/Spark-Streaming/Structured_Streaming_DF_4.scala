package Spark_Structured_Streaming

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

object Structured_Streaming_DF_4 {
  println("Spark Structured Streaming DF 4")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName("Spark Structured Streaming DF 4")
    .getOrCreate()

//  define schema for bands
  val bands_schema = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("hometown", StringType),
    StructField("year", IntegerType)
  ))

//  define schema for guitar
  val guitar_schema = StructType(Array(
    StructField("id", IntegerType),
    StructField("model", StringType),
    StructField("make", StringType),
    StructField("guitarType", StringType)
  ))

  def streamwithstream() = {
    val bands_stream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), bands_schema).as("bands"))
      .selectExpr("bands.id as id", "bands.name as name", "bands.hometown as hometown", "bands.year as year")

    val guitars_stream = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 2302)
      .load()
      .select(from_json(col("value"), guitar_schema).as("guitars"))
      .selectExpr("guitars.id as id", "guitars.model as model", "guitars.make as make",
        "guitars.guitarType as guitarType")

    val join_cond = bands_stream.col("id") === guitars_stream.col("id")
    val stream_with_stream = bands_stream.join(guitars_stream, join_cond, "inner")

    stream_with_stream.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    streamwithstream()
  }
}
