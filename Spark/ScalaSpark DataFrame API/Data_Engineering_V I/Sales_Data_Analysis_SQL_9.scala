package Data_Engineering_5

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

object Sales_Data_Analysis_SQL_9 {
  println("Sales Data Analysis 9")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 9")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  def read_sales_data(filename: String) = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv(s"D:/DataSet/DataSet/SparkDataSet/$filename")

  import spark.implicits._
  val sales_df = read_sales_data("Sales_Data.csv")
  sales_df.show(false)

  def compute() = {
    println(s"COMPUTE MAX SALES IN EACH COUNTRY FOR EACH PRODUCT IN EACH YEAR")
    val country_df = sales_df.selectExpr("COUNTRY as Country", "PRODUCTLINE as Product", "YEAR_ID as Year", "SALES as Sales")
      .groupBy("Country", "Year", "Product")
      .agg(
        sum("Sales").as("Sales")
      ).orderBy(asc("Year"), desc("Sales"))
    country_df.show(false)

    import spark.implicits._
    val top_sales = country_df.select($"Country", $"Year", $"Product", $"Sales")
      .withColumn("Rank", rank().over(Window.partitionBy($"Country", $"Year").orderBy(desc("Sales"))))
    top_sales.show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}