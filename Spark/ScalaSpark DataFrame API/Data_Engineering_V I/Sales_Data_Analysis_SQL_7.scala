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

object Sales_Data_Analysis_SQL_7 {
  println("Sales Data Analysis 7")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 7")
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

  def compute() = {
    import spark.implicits._
    val sales_df = read_sales_data("Sales_Data.csv")
    sales_df.show(false)

    println(s"COMPUTE SALES FOR EACH ORDER NUMBER")
    val qty_df = sales_df.selectExpr("YEAR_ID as Year", "ORDERNUMBER as Order_NO", "SALES as Sales")
      .groupBy("Year", "Order_NO")
      .agg(
        sum("Sales").as("Sales")
      ).orderBy(asc("Year"), desc("Sales"))
      .where("Year in (2003, 2004, 2005)")
    qty_df.show(200,false)

    val top_selling_qty = qty_df.selectExpr("Year", "Order_NO", "Sales")
      .withColumn("Rank", rank().over(Window.partitionBy("Year").orderBy(desc("Sales"))))
      .where("Rank = 1")
    top_selling_qty.show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}