package BigData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

object Cars_Analysis_2 {
  println("USA Cars Details using StructType")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("USA Cars Details using StructType")
    .getOrCreate()

// define schema for cars DF
  val cars_schema = StructType(List(
    StructField("ID", IntegerType),
    StructField("price", IntegerType),
    StructField("brand", StringType),
    StructField("model", StringType),
    StructField("year", IntegerType),
    StructField("title_status", StringType),
    StructField("mileage", StringType),
    StructField("color", StringType),
    StructField("vin", StringType),
    StructField("lot", IntegerType),
    StructField("state", StringType),
    StructField("country", StringType),
    StructField("condition", StringType)
  ))

  def compute() = {
    val cars_df = spark.read
      .format("csv")
      .option("header", "true")
      .schema(cars_schema)
      .load("D:/DataSet/DataSet/SparkDataSet/cars_USA.csv")

    cars_df.show(10, false)
    cars_df.printSchema()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
