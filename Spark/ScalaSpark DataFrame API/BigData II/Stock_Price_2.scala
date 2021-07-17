package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._

object Stock_Price_2 extends App {
  println("Stock Price Analysis 2")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Stock Price Analysis")
    .getOrCreate()

  //  define schema for stock
  val stock_schema = StructType(Array(
    StructField("StkDate", DateType),
    StructField("Open", DoubleType),
    StructField("High", DoubleType),
    StructField("Low", DoubleType),
    StructField("Close", DoubleType),
    StructField("AdjClose", DoubleType),
    StructField("Volume", DoubleType)
  ))
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  //  spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
  //  https://stackoverflow.com/questions/62602720/string-to-date-migration-from-spark-2-0-to-3-0-gives-fail-to-recognize-eee-mmm

  //  read the csv file into DF
  val stock_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(stock_schema)
    .option("dateFormat", "YYYY-dd-MM")
    .option("mode", "failFast") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/Code/DataSet/SparkDataSet/StockPrice.csv")
    .load()

  stock_df.printSchema()
  stock_df.show()

  val cln_stk_df = stock_df.select(
    col("StkDate"),
    dayofmonth(col("StkDate")).alias("Day"),
    month(col("StkDate")).alias("Month"),
    year(col("StkDate")).alias("Year"),
    round(col("Open"),2).alias("Open"),
    round(col("High"),2).alias("High"),
    round(col("Low"),2).alias("Low"),
    round(col("Close"),2).alias("Close"),
    round(col("AdjClose"),2).alias("AdjClose"),
    round(col("Volume"),2).alias("Volume")
    )
  cln_stk_df.show()
}
