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

object Sales_Data_Analysis_SQL_5 {
  println("Sales Data Analysis 5")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Sales Data Analysis 5")
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

    println(s"COMPUTE THE DEAL SIZE THAT HAPPENED IN EACH YEAR")
    val deal_df = sales_df.selectExpr("DEALSIZE as Deal", "YEAR_ID as Year")
      .groupBy("Year","Deal")
      .agg(
        count("Deal").as("Deal_Cnt")
      ).sort(asc("Year"),desc("Deal_Cnt"))
    deal_df.show(false)

    val top_deal = deal_df.select("Year", "Deal", "Deal_Cnt")
      .withColumn("Rank", rank().over(Window.partitionBy("Year").orderBy(desc("Deal_Cnt"))))
      .show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}