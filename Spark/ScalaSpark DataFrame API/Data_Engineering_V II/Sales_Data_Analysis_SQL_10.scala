package Data_Engineering_V

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

object Sales_Data_Analysis_SQL_10 {
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
    .csv(s"D:/Code/DataSet/SparkDataSet/$filename")

  import spark.implicits._
  val sales_df = read_sales_data("Sales_Data.csv")
  sales_df.show(false)

  def compute() = {
    println(s"COMPUTE MAX SALES IN EACH COUNTRY FOR EACH PRODUCT IN EACH YEAR")
    val order_df = sales_df.createOrReplaceTempView("Sales_Temp")
    val order_stat_df = spark.sql(
      """
        |select
        |year_id as Year,
        |productline as Product,
        |ordernumber as Order,
        |sum(sales) over (partition by ordernumber, productline order by sales desc) as Total_Sales,
        |rank() over (partition by ordernumber, productline order by sales desc) as Rank
        |from sales_temp
        |""".stripMargin)
      .show(100,false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
