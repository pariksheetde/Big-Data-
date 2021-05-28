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

object Spark_Kafka_DF_2 {
  println("Spark Integration with Kafka 2")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 2")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

//  define cars schema
  val cars_schema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", DoubleType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", DoubleType),
    StructField("Weight_in_lbs", DoubleType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType),
  ))

//  reading from Kafka and writing to kafka
  def write_to_kafka() = {
    val cars_df = spark.readStream
      .schema(cars_schema)
      .json("src/main/resources/cars")

    val cars_kafka_df = cars_df.selectExpr("upper(Name) as key", "Name as value")


//  reading from Kafka and writing to kafka
    cars_kafka_df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "checkpoint") // without checkpoint writing will fail on kafka
//      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    write_to_kafka()
  }
}
