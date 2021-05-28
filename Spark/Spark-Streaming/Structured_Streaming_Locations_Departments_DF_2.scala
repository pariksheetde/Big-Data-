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

object Structured_Streaming_Locations_Departments_DF_2 {
  println("Spark Structured Streaming Locations-Departments DF 2")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName("Spark Structured Streaming Locations-Departments DF 2")
    .getOrCreate()

//  define schema for locations.json
  val location_schema = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))

//  define schema for departments.json
  val departments_schema = StructType(Array(
    StructField("department_id", IntegerType),
    StructField("department_name", StringType),
    StructField("location_id", IntegerType)
  ))


  def batch_stream_join() = {
    val stream_location_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), location_schema).as("locations"))
      .selectExpr("locations.location_id as location_id", "locations.location_name as location_name")

    val departments_df = spark.read
      .format("json")
      .schema(departments_schema)
      .load("src/main/resources/departments")

    val loc_dept_cond = stream_location_df.col("location_id") === departments_df.col("location_id")
    val loc_dept_join = stream_location_df.join(departments_df, loc_dept_cond, "inner")

    loc_dept_join.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    batch_stream_join()
  }
}
