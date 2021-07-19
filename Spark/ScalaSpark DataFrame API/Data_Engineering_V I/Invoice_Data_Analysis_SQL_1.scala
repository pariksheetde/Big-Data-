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

object Invoice_Data_Analysis_SQL_1 {
  println("Invoice Data Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Invoice Data Analysis 1")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  def read_invoice_fn(filename: String) = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv(s"D:/DataSet/DataSet/SparkData/$filename")

  def compute() = {
    import spark.implicits._
    val invoice_df = read_invoice_fn("Invoices.csv")
    invoice_df.show(false)

    println(s"COMPUTE THE INVOICE STATS")
    val inv_agg = invoice_df.select("Country", "InvoiceNo")
      .groupBy("Country")
      .agg(
        count("Country").as("Cnt_of_Records"),
        countDistinct("Country").as("Number_of_Country"),
        count("InvoiceNo").as("Number_of_Invoices_each_Country")
      )
    inv_agg.show(false)

    val cnt_invoice_country = invoice_df.groupBy("Country", "InvoiceNo")
      .agg(
        count("InvoiceNo").as("Invoice_Cnt")
      ).show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
