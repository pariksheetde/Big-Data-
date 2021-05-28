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


object Spark_Kafka_DF_6 {
  println("Spark Integration with Kafka 6")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 6")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  //  define schema for cars
  val cars_schema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", IntegerType),
    StructField("Displacement", IntegerType),
    StructField("Horsepower", IntegerType),
    StructField("Acceleration", DoubleType),
    StructField("Weight_in_lbs", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

// read from json file
  def writetoKafka() = {
    val kafka_df = spark.readStream
      .format("json")
      .schema(cars_schema)
      .load("src/main/resources/cars")

//  transformation logic
  val trns_df = kafka_df.select(
    col("Name").as("key"),
    to_json(struct(col("Name"),col("Year"), col("Displacement"),
      col("Origin"), col("Horsepower"))).cast("String").alias("value"))

    //  write to kafka consumer
    val write_df = trns_df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "chk-point-dir")
      .start()
      .awaitTermination()
  }



  def main(args: Array[String]): Unit = {
    writetoKafka()
  }
}
