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


object Structured_Streaming_DF_2 {
  println("Spark Structured Streaming DF 1")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .appName("Spark Structured Streaming DF 1")
    .getOrCreate()

//  define schema for stocks file

val stock_schema = StructType(Array(
  StructField("Name", StringType),
  StructField("Date", StringType),
  StructField("value", DoubleType)
))

//  readstream
  def readfromFiles() = {
    val stock_df = spark.readStream
      .schema(stock_schema)
      .format("csv")
      .option("dateFormat", "MM d YYYY")
      .option("header", "false")
      .load("D:/DataSet/SparkStreaming")

//  writestream
    val writer = stock_df.writeStream
      .format("csv")
      .option("path","D:/DataSet/SparkStreaming/SparkStreamingSource/Stocks")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("10 seconds")) // every 10 seconds the spark will run the query
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    readfromFiles()
  }
}
