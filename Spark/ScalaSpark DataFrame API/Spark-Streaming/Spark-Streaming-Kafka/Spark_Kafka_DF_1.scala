package Spark_Streaming_Kafka

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration
import scala.concurrent.duration.DurationInt

object Spark_Kafka_DF_1 {
  println("Spark Integration with Kafka 1")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 1")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

//  reading from Kafka and writing to console
  def read_from_kafka() = {
    val kafka_df : DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()
      .select(col("topic"), expr("cast(value as string) as message"))


//  reading from kafka and writing to console
    kafka_df.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    read_from_kafka()
  }
}
