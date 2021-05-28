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

object Structured_Streaming_DF_1 {
  println("Spark Structured Streaming DF 1")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .appName("Spark Structured Streaming DF 1")
    .getOrCreate()

  def readfromSocket() = {
    val lines_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()

//  transformation logic
//    val filter_df = lines_df.filter(length(col("value")) >= 5)
    val agg = lines_df.groupBy(col("value")).count()

//  sink
    val qry_df = agg.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readfromSocket()
  }
}
