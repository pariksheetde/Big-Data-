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

object Sales_Data_Analysis_SQL_1 {
  println("Sales Data Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 1")
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
    println(s"COMPUTE THE STATS FOR SALES DATA")
    println(s"Number of Orders made in each year")
    val qty_cnt = sales_df.createOrReplaceTempView("Sales_Details")
    spark.sql(
      """
        |select
        |year_id,
        |count(quantityordered) as qty_cnt
        |from sales_details
        |group by year_id
        |""".stripMargin)
      .show()

    //  println(s"Number of products purchased in each year")
    val purchase_qty = sales_df.createOrReplaceTempView("Purchase_Details")
    spark.sql(
      """
        |select
        |year_id,
        |count(productline) as product_cnt,
        |productline
        |from purchase_details
        |group by year_id, productline
        |order by year_id
        |""".stripMargin)
      .show(false)

    println(s"Number of status for each product purchased in each year")
    val status_purchase = sales_df.createOrReplaceTempView("Status_Details")
    spark.sql(
      """
        |select
        |year_id,
        |productline as product_name,
        |status,
        |count(productline) as product_cnt
        |from status_details
        |group by year_id, status, product_name
        |order by year_id
        |""".stripMargin)
      .show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
