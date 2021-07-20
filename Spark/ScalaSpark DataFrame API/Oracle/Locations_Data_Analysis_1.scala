package Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

import java.sql.Struct


object Locations_Data_Analysis_1 {
  println("Accessing Oracle DB")
  println("Accessing Locations Table")


  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Accessing Oracle DB")
    .getOrCreate()

  val locations_schema = StructType(Array(
    StructField("LOCATION_ID", DoubleType),
    StructField("STREET_ADDRESS", StringType),
    StructField("POSTAL_CODE", StringType),
    StructField("CITY", StringType),
    StructField("STATE_PROVINCE", StringType),
    StructField("COUNTRY_ID", StringType),
  ))

  val loc_df = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:Hr/Hr@localhost:1521/prod")
    .option("dbtable", "hr.locations")
    .option("user", "Hr")
    .option("password", "Hr")
//    .schema(locations_schema)
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()

  def compute() = {
    loc_df.printSchema()
    loc_df.show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
