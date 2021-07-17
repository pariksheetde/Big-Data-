package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType, FloatType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}

object Video_Games_Analysis_2 {
  println("Video Games Analysis 2")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Video Games Analysis")
    .getOrCreate()

  //  define schema for video games DF
  val vg_schema = StructType(Array(
    StructField("Rank", IntegerType),
    StructField("Name", StringType),
    StructField("Platform", StringType),
    StructField("Year", IntegerType),
    StructField("Genre", StringType),
    StructField("Publisher", StringType),
    StructField("NA_Sales", FloatType),
    StructField("EU_Sales", FloatType),
    StructField("JP_Sales", FloatType),
    StructField("Other_Sales", FloatType),
    StructField("Global_Sales", FloatType)
  ))

  //  read the video games DF
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  val games_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(vg_schema)
//  .option("dateFormat", "yyyy-MM-dd")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive, failFast
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/DataSet/DataSet/SparkDataSet/vgsales.csv")
    .load()

  def compute() = {
    val total_sales_df = games_df
      .select((col("NA_Sales") + col("EU_Sales") + col("JP_Sales") +
        col("Other_Sales") + col("Global_Sales")).as("Total_Sales"), col("Name"))

      total_sales_df.show(truncate = false)
      println(s"Records Effected: ${total_sales_df.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
