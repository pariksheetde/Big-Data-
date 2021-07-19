package Data_Engineering_4

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

object Sales_Data_Analysis_1 {
  println("Sales Data Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 1")
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
    .csv("D:/DataSet/DataSet/SparkDataSet/Tesco_Sales.csv")

  sales_df.printSchema()
  sales_df.show(false)

  //  count number of records in the cars_df DF
  println(s"Number of records in the sales_df DF: ${sales_df.count()}")

  def compute() = {
    //  count number of countries
    println(s"Number of Countries")
    import spark.implicits._
    val cnt_distinct_country = sales_df.select($"Country")
      .groupBy($"Country")
      .agg(
        count(col("Country")).alias("Country_Cnt")
      ).show(false)

    //  count number of Cities
    println(s"Number of Cities")
    import spark.implicits._
    val cnt_distinct_city = sales_df.select($"City")
      .groupBy($"City")
      .agg(
        count(col("City")).alias("City_Cnt")
      ).sort($"City_Cnt".desc_nulls_last)
      .show(false)

    //  count number of Segment
    println(s"Number of Segment")
    import spark.implicits._
    val cnt_segment = sales_df.select($"Segment")
      .groupBy($"Segment")
      .agg(
        count(col("Segment")).alias("Segment_Cnt")
      ).sort($"Segment_Cnt".desc_nulls_last)
      .show(false)

    //  count number of Segment
    println(s"Number of Ship Mode")
    import spark.implicits._
    val cnt_ship_mode = sales_df.select($"Ship_Mode")
      .groupBy($"Ship_Mode")
      .agg(
        count(col("Ship_Mode")).alias("ShipMode_Cnt")
      ).sort($"ShipMode_Cnt".desc_nulls_last)
      .show(false)

    //  count number of Segment
    println(s"Number of Customers")
    import spark.implicits._
    val cnt_customer = sales_df.select($"CustomerID")
      .groupBy($"CustomerID")
      .agg(
        count(col("CustomerID")).alias("Customer_Cnt")
      ).sort($"Customer_Cnt".desc_nulls_last)
      .show(false)

    //  count number of orders received
    println(s"Number of Orders Received")
    import spark.implicits._
    val cnt_orders = sales_df.select($"OrderID")
      .groupBy($"OrderID")
      .agg(
        count(col("OrderID")).alias("Orders_Cnt")
      ).sort($"Orders_Cnt".desc_nulls_last)
      .show(false)

    //  count number of orders category
    println(s"Number of Product Category")
    import spark.implicits._
    val cnt_category = sales_df.select($"Category")
      .groupBy($"Category")
      .agg(
        count(col("Category")).alias("Category_Cnt")
      ).sort($"Category_Cnt".desc_nulls_last)
      .show(false)

    //  count number of orders sub category
    println(s"Number of Product Category")
    import spark.implicits._
    val cnt_sub_category = sales_df.select($"Sub_Category")
      .groupBy($"Sub_Category")
      .agg(
        count(col("Sub_Category")).alias("Sub-Category_Cnt")
      ).sort($"Sub-Category_Cnt".desc_nulls_last)
      .show(false)

    //  calculate sales in each city
    println(s"Calculate sales in each city")
    val city_sales = sales_df.select($"City", $"Category", $"Sales")
      .groupBy("City")
      .agg(
        sum("Sales").as("Sum_Sales")
      )
      .where("City = 'Tyler'")
      .sort("Sum_Sales")
      .show(false)
    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}