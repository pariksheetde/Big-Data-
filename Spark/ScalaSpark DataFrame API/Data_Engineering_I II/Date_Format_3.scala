package Data_Engineering_I

import Data_Engineering_I.Date_Format_1.spark
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.monotonically_increasing_id

object Date_Format_3 extends App {
  println("Date Format 2 using StructType")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  val datalist = List(
    ("Monica", "12", "01", "2002"),
    ("Kate", "14", "10", "81"), // 1981
    ("Pamela", "16", "12", "06"), // 2006
    ("Peter", "25", "05", "63"), // 1963
    ("Kylie", "19", "09", "85"), //1985
  )

  val df = spark.createDataFrame(datalist).toDF("Name", "Date", "Month", "Year")

  df.printSchema()
  df.show()

  val res_df = df.withColumn("id", monotonically_increasing_id())
    .withColumn("Date", expr("Date").cast(IntegerType))
    .withColumn("Month", expr("Month").cast(IntegerType))
    .withColumn("Year", expr("Year").cast(IntegerType))
    .withColumn("Year", expr(
      """
        |case when Year < 21 then cast(Year as Int) + 2000
        |when Year < 100 then cast(Year as Int) + 1900
        |else Year
        |end
        |""".stripMargin))

  res_df.printSchema()
  res_df.show()

}
