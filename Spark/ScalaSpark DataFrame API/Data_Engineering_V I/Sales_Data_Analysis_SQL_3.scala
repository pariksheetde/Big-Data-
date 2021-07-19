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

object Sales_Data_Analysis_SQL_3  {
  println("Sales Data Analysis 3")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 3")
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
    println(s"COMPUTE THE STATUS FOR YEAR PRODUCT IN EACH YEAR")
    val status = sales_df.selectExpr( "YEAR_ID as Year", "STATUS as Status", "PRODUCTLINE as Product", "sales as Sales")
      .groupBy("Year","Status", "Product")
      .agg(
        sum("sales").alias("Total_Sales")
      ).orderBy(asc("Year"), desc("Total_Sales"))
    status.show(false)

    val tab_status = status.createOrReplaceTempView("Status_Temp")
    val sql_status = spark.sql(
      """select Year, Status, Product,
        |Total_Sales,
        |rank() over (partition by year, status order by total_sales desc) as Rank
        |from Status_Temp
        |order by year asc, total_sales desc""".stripMargin)
      .show(1000,false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}