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

object Sales_Data_Analysis_SQL_2 {
  println("Sales Data Analysis 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 2")
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

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val sales_df = read_sales_data("Sales_Data.csv")
    sales_df.show(false)

    println(s"COMPUTE THE STATS FOR SALES DATA FOR EACH PRODUCT")
    println(s"Compute Sales for all products in the year 2003")
    val sales_product_i = sales_df.createOrReplaceTempView("Sales_I")
    spark.sql(
      """select * from
      (select
        |year_id as year,
        |productline as product,
        |sales,
        |max(sales) over(partition by year_id, productline order by sales desc) as sales,
        |rank() over(partition by year_id, productline order by sales desc) as rank
        |from Sales_I
        |order by year_id, sales desc) temp where rank = 1
        |""".stripMargin)
      .show(1000,false)

    println(s"Compute Sales for all products in each year")
    val sales_product_ii = sales_df.createOrReplaceTempView("Sales_II")
    val top_product = spark.sql(
      """select
        |year, product,
        |max(sum_sales) over (partition by year, product) as max_sales,
        |rank() over (partition by year, product order by sum_sales) as outer_rank
        |from
      (select
        |year_id as year,
        |productline as product,
        |sum(sales) over(partition by year_id, productline order by sales asc) as sum_sales,
        |rank() over(partition by year_id, productline order by sales asc) as rank
        |from sales_ii
        |order by year_id, sum_sales desc)
        |temp
        |""".stripMargin)

    import spark.implicits._
    val top_sales = top_product
      .filter("outer_rank = 1")
      .orderBy(desc("Year"))
      .show(1000,false)
    spark.stop()
  }
}