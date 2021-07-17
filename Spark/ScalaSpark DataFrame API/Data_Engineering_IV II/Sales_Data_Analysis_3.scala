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

object Sales_Data_Analysis_3 {
  println("Sales Data Analysis 3")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 3")
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

  sales_df.show(false)

  def compute() = {
    //  top performing sales in each city based on category
    import spark.implicits._
    val top_city_sales_1 = sales_df.select($"City", $"Category", $"Sales")
      .withColumn("Max_Sales", max("Sales").over(Window.partitionBy($"City").orderBy($"Sales".desc)))
      .withColumn("Rank", row_number().over(Window.partitionBy($"City").orderBy($"Max_Sales".desc)))
      .where("City = 'Jacksonville'") // Detroit, New York City, Chicago, Dallas, Jacksonville
      .orderBy($"Sales".desc)
      .show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
