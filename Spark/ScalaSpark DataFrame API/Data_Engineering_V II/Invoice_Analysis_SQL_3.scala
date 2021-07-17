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

object Invoice_Analysis_SQL_3 {
  println("Invoice Data Analysis 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Invoice Data Analysis 2")
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
    .csv(s"D:/Code/DataSet/SparkData/$filename")

  import spark.implicits._
  val invoice_df = read_invoice_fn("Invoices.csv")
  invoice_df.show(false)

  val cln_invoice_df = invoice_df.selectExpr("InvoiceNo", "Quantity", "InvoiceDate", "UnitPrice", "Country")
    .withColumn("Invoice_DD", substring(col("InvoiceDate"), 1, 2))
    .withColumn("Invoice_MM", substring(col("InvoiceDate"), 4, 2))
    .withColumn("Invoice_YYYY", substring(col("InvoiceDate"), 7, 4))
    .withColumn("Invoice_DD", col("Invoice_DD").cast(IntegerType))
    .withColumn("Invoice_MM", col("Invoice_MM").cast(IntegerType))
    .withColumn("Invoice_YYYY", col("Invoice_YYYY").cast(IntegerType))
    .withColumn("Invoice_DT", to_date(expr("concat(Invoice_DD, '/', Invoice_MM, '/', Invoice_YYYY)"),"d/M/y"))
    .withColumn("Week", weekofyear(col("Invoice_DT")))
  cln_invoice_df.printSchema()
  cln_invoice_df.show(false)

  def compute() = {
    val invoice_agg = cln_invoice_df.selectExpr("Country", "Week", "Quantity", "UnitPrice", "InvoiceNo")
      .groupBy("Country", "Week")
      .agg(
        sum(col("UnitPrice") * col("Quantity")).alias("InvoiceValue"),
        sum(col("Quantity")).as("TotalQuantity"),
        count(col("InvoiceNo")).as("NumInvoices")
      )
    invoice_agg.show(false)

    val invoice_win = invoice_agg.selectExpr("Country", "Week", "NumInvoices", "TotalQuantity", "InvoiceValue")
      .withColumn("RunningTotal", sum("InvoiceValue").over(Window.partitionBy(col("Country")).orderBy(desc("Week"))))
      .where("Country = 'EIRE' and week in (48, 49, 50, 51)")
      .show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
