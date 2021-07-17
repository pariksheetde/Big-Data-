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

object Sales_Data_Analysis_SQL_4 {
  println("Sales Data Analysis 4")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 4")
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

  def compute() = {
    import spark.implicits._
    val sales_df = read_sales_data("Sales_Data.csv")
    sales_df.show(false)

    println(s"COMPUTE THE PRODUCT FOR YEAR STATUS IN EACH YEAR")
    val status_df = sales_df.selectExpr("PRODUCTLINE as Product", "STATUS as Status", "Sales as Sales")
      .groupBy("Product", "Status")
      .agg(
        sum("Sales").alias("Sum_Sales")
      ).sort(asc("Product"), desc("Sum_Sales"))
    status_df.show(false)

    val temp_status = status_df.createOrReplaceTempView("Status_Temp")
    val top_status = spark.sql(
      """
        |select
        |product, status, sum_sales,
        |rank() over (partition by product order by sum_sales desc) as Rank
        |from status_temp
        |order by product asc, sum_sales desc
        |""".stripMargin)
      .show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
