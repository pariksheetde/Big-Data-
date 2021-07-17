package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}

object Amazon_Books_Analysis_1 {
  println("Amazon Books Analysis 1")

//  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Amazon Books Analysis 1")
    .getOrCreate()

//  define schema for books DF
  val books_schema = StructType(Array(
    StructField("Name", StringType),
    StructField("Author", StringType),
    StructField("User_Rating", DoubleType),
    StructField("Reviews", IntegerType),
    StructField("Price", IntegerType),
    StructField("Year", IntegerType),
    StructField("Genre", StringType)
  ))

//  read the books DF
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  val books_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(books_schema)
    .option("mode", "failFast") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv("D:/Code/DataSet/SparkDataSet/amazon_books.csv")

  def main(args: Array[String]): Unit = {
    //  compute the max of price, min of price, avg of price, count(*) of rows for each genre
    val agg_price = books_df.select("Genre", "Price")
      .groupBy("Genre")
      .agg(max("Price").alias("Max_Price"),
        mean("Price").alias("Min_Price"),
        avg("Price").as("Avg_Price"),
        count("*").as("Cnt"))
      .orderBy("Max_Price")

    val agg_price_show = agg_price.show(agg_price.count().toInt)
    println(s"Records Effected: ${agg_price.count()}")

    spark.stop()
  }

}
