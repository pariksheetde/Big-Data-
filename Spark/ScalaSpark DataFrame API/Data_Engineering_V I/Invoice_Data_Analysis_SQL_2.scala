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

object Invoice_Data_Analysis_SQL_2 {
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
    .csv(s"D:/DataSet/DataSet/SparkData/$filename")

  import spark.implicits._
  val invoice_df = read_invoice_fn("Invoices.csv")
  invoice_df.show(false)

  def compute() = {
    val cln_invoice_df = invoice_df.selectExpr("InvoiceNo", "Quantity", "InvoiceDate", "UnitPrice", "Country")
      .withColumn("Invoice_DD", substring(col("InvoiceDate"), 1, 2))
      .withColumn("Invoice_MM", substring(col("InvoiceDate"), 4, 2))
      .withColumn("Invoice_YYYY", substring(col("InvoiceDate"), 7, 4))
      .withColumn("Invoice_DD", col("Invoice_DD").cast(IntegerType))
      .withColumn("Invoice_MM", col("Invoice_MM").cast(IntegerType))
      .withColumn("Invoice_YYYY", col("Invoice_YYYY").cast(IntegerType))
      .withColumn("Invoice_DT", to_date(expr("concat(Invoice_DD, '/', Invoice_MM, '/', Invoice_YYYY)"), "d/M/y"))
      .withColumn("Week", weekofyear(col("Invoice_DT")))
    cln_invoice_df.printSchema()
    cln_invoice_df.show(false)

    val invoice_agg = cln_invoice_df.selectExpr("Country", "Week", "Quantity", "UnitPrice", "InvoiceNo")
      .groupBy("Country", "Week")
      .agg(
        round(sum(col("Quantity") * col("UnitPrice")), 2).as("InvoicePrice"),
        sum("Quantity").as("TotalQuantity"),
        countDistinct("InvoiceNo").alias("NumInvoice")
      )
    invoice_agg.show(false)

    val invoice_output = invoice_agg.coalesce(1)
      .write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .save("D:/DataSet/OutputDataset/Invoice")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}