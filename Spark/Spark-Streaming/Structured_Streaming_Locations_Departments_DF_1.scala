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

object Structured_Streaming_Locations_Departments_DF_1 {
  println("Spark Structured Streaming Locations-Departments DF 1")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName("Spark Structured Streaming Locations-Departments DF 1")
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

  val locations_df = spark.readStream
    .format("json")
    .schema(location_schema)
    .load("src/main/resources/locations")

  val departments_df = spark.readStream
    .format("json")
    .schema(departments_schema)
    .load("src/main/resources/departments")

  def loc_dept() = {
    val loc_dept_join_cond = locations_df.col("location_id") === departments_df.col("location_id")
    val loc_dept_join = locations_df.join(departments_df, loc_dept_join_cond, "inner")
      .drop(departments_df.col("location_id"))

    val final_df = loc_dept_join.selectExpr("location_id", "department_id", "department_name", "location_name")

    final_df.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    loc_dept()
  }
}
