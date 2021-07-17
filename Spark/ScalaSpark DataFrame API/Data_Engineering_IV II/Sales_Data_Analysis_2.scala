package Data_Engineering_IV

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

object Sales_Data_Analysis_2 {
  println("Sales Data Analysis 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 2")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

//  define schema for sales
  val sales_schema = StructType(List(
    StructField("RowID", IntegerType),
    StructField("OrderID", StringType),
    StructField("Order_Date", StringType),
    StructField("Ship_Date", StringType),
    StructField("Ship_Mode", StringType),
    StructField("CustomerID", StringType),
    StructField("Customer_Name", StringType),
    StructField("Segment", StringType),
    StructField("Country", StringType),
    StructField("City", StringType),
    StructField("State", StringType),
    StructField("Postal_Code", IntegerType),
    StructField("Region", StringType),
    StructField("Product_ID", StringType),
    StructField("Category", StringType),
    StructField("Sub_Category", StringType),
    StructField("Product_Name", StringType),
    StructField("Sales", DoubleType)
  ))

  //  read the movies.json datafile from the location
  val sales_df = spark.read
    .option("header", "true")
    .schema(sales_schema)
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv("D:/Code/DataSet/SparkDataSet/Tesco_Sales.csv")


  def compute() = {
    //  calculate total sales for each category
    println(s"Sum of sales in each category")
    import spark.implicits._
    val sales_category = sales_df.select($"Category", col("Sales"))
      .groupBy($"Category")
      .agg(
        sum(col("Sales")).as("Sum_Sales_Category")
      ).sort(col("Category").desc_nulls_last)
      .show(false)

    //  calculate total sales for each category and sub-category
    import spark.implicits._
    println(s"Sum of sales in each category and sub_category")
    val sales_cat_sub_cat = sales_df.select($"Category", $"Sub_Category", $"Sales")
      .groupBy(col("Category"), col("Sub_Category"))
      .agg(
        round(sum("Sales"), 2).alias("Sum_Sales_Sub_Category")
      ).sort(col("Category").desc_nulls_last)
      .show(false)

    //  number of customers in each city
    println(s"Number of customers in each city")
    import spark.implicits._
    val cust_cnt = sales_df.select($"CustomerID", col("City"), col("Sales"))
      .groupBy(col("City"))
      .agg(
        count("CustomerID").as("Cust_Cnt"),
        round(sum(col("Sales")), 2).alias("Sum_Sales_City")
      ).orderBy($"Cust_Cnt".desc_nulls_last)
      .show(false)

    //  find the sum of sales for each category where City is Tyler
    import spark.implicits._
    val sum_sales_tyler = sales_df.select(col("City"), $"Category", $"Sales")
      .groupBy("City")
      .agg(
        sum($"Sales").as("Sum_Sales")
      )
      .where("City = 'Tyler'")
      .show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
