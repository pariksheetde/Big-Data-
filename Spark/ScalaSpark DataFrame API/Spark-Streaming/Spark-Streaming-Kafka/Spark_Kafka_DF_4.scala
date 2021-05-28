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


object Spark_Kafka_DF_4 {
  println("Spark Integration with Kafka 4")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 4")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

//  read from kafka
  def readfromKafka() = {
    val kafka_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "rockthejvm")
      .load()
      .select(col("timestamp"), expr("cast(value as string) as message"))
      .withColumn("marks", col("message").cast("Int"))
      .drop(col("message"))
    kafka_df.printSchema()

// transformation logic
    val trns_df = kafka_df.agg(
      sum(col("marks")).alias("sum"),
      avg(col("marks")).alias("avg"),
      min(col("marks")).alias("min"),
      count(col("marks")).as("cnt")
    )

//  write to console
  val output_df = trns_df.writeStream
    .format("console")
    .outputMode("update")
    .option("checkpointLocation", "chk-point-dir")
    .start()
    .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    readfromKafka()
  }
}
