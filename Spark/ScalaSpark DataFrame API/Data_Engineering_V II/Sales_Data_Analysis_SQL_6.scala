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

object Sales_Data_Analysis_SQL_6  {
  println("Sales Data Analysis 6")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 6")
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

    println(s"CALCULATE THE SALES FOR EACH DEALSIZE IN EACH YEAR")
    val deal_df = sales_df.selectExpr("DEALSIZE as Deal",  "STATUS as Status", "YEAR_ID as Year","SALES as Sales")
      .groupBy("Year", "Status", "Deal")
      .agg(
        sum("Sales").alias("Sum_Sales")
      ).sort(asc("Year"), desc("Sum_Sales"))
    deal_df.show(false)

    println(s"COMPUTE THE DEAL FOR EACH STATUS SIZE FOR THE YEAR 2003")
    val top_deal_2003 = deal_df.selectExpr("Year", "Status", "Deal", "Sum_Sales")
      .withColumn("Rank", rank()over(Window.partitionBy("Year", "Status").orderBy(desc("Sum_Sales"))))
      .filter($"Year" === 2003)
    top_deal_2003.show(false)

    println(s"COMPUTE THE DEAL FOR EACH STATUS SIZE FOR THE YEAR 2004")
    val top_deal_2004 = deal_df.selectExpr("Year", "Status", "Deal", "Sum_Sales")
      .withColumn("Rank", rank()over(Window.partitionBy("Year", "Status").orderBy(desc("Sum_Sales"))))
      .filter($"Year" === 2004)
    top_deal_2004.show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
