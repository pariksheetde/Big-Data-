package Data_Engineering_1

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._

object Date_Format_1 extends App {
  println("Date Format 1 using StructType")
  println("Date Format 1")

  //  create a function that could convert the string type to date type
  def dateconversion(df: DataFrame, fmt: String, fld: String): DataFrame = {
    df.withColumn(fld, to_date(col(fld),fmt))
  }

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  import spark.implicits._
  val columns = Seq("ID","EventDate")
  val data = Seq((100, "01/01/2020"), (110, "02/02/2020"), (120, "03/03/2020"))
  val df = spark.createDataFrame(data).toDF(columns:_*)

  val def_schema = StructType(List(
    StructField("ID", IntegerType, true),
    StructField("EventDate", StringType, true)
  )
  )
  println("Before Conversion")
  df.printSchema()
  df.show()

  //  after conversion from string type to date type
  val func = dateconversion(df, "M/d/y", "EventDate")
  println("After Conversion")
  func.printSchema()
  func.show(10)

  spark.stop()
}