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

object Sales_Data_Analysis_4 {
  println("Sales Data Analysis 4")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 4")
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

  sales_df.printSchema()
  sales_df.show(false)

  def compute() = {
    //  compute the highest sales for category and sub-category
    import spark.implicits._
    println(s"Top Sales in each city, category, sub_category")
    val sales_cat_subcat = sales_df.select($"City", $"Category", $"Sub_Category", $"Sales")
      .withColumn("Rank",row_number().over(Window.partitionBy($"City", $"Category", $"Sub_Category").orderBy($"Sales".desc)))
      .sort($"Sales".desc)
      .where("Category = 'Technology' and Sub_Category = 'Copiers' and City = 'Los Angeles'")
      .show(1000,false)

    //  select those customers who have made maximum purchase for each OrderID, Category
    import spark.implicits._
    println(s"select those customers who have made maximum purchase for each OrderID, Category")
    val puchase_order_category_cnt = sales_df.select('CustomerID,  $"OrderID", 'Category, $"Sales")
      .withColumn("Max_Sales", max('Sales).over(Window.partitionBy('OrderID, $"Category").orderBy('Sales.desc)))
      .withColumn("Rank", rank().over(Window.partitionBy('OrderID, $"Category").orderBy($"Sales".desc)))
      .where("CustomerID in ('AA-10315')") // 'AA-10375'
      .show()

    //  select those customers who have made maximum purchase for each OrderID
    import spark.implicits._
    println(s"select those customers who have made maximum purchase for each OrderID")
    val puchase_order_cnt = sales_df.select('CustomerID,  $"OrderID", $"Sales")
      .withColumn("Max_Sales", max('Sales).over(Window.partitionBy('OrderID).orderBy('Sales.desc)))
      .withColumn("Rank", rank().over(Window.partitionBy('OrderID).orderBy($"Sales".desc)))
      .where("CustomerID in ('AA-10315')") // 'AA-10375'
      .show()

    //  select those customers who have made maximum purchase for each OrderID
    import spark.implicits._
    println(s"select those customers who have made maximum purchase for each OrderID")
    val sum_sales = sales_df.select('CustomerID,'Category, $"Sales")
      .withColumn("Sum_Sales", sum('Sales).over(Window.partitionBy('CustomerID).orderBy('Sales.desc)))
      .withColumn("Rank", rank().over(Window.partitionBy('CustomerID).orderBy($"Sum_Sales".desc)))
      .where("CustomerID in ('AA-10315', 'AA-10375')") // 'AA-10375'
      .filter("Rank = 1")
      .show(1000, false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
