package Spark_Structured_Streaming

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
import org.apache.spark.sql.streaming.Trigger


object Structured_Streaming_DF_5 {
  println("Spark Structured Streaming DF 5")
  println("Aggregating Invoice.json")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .config("maxFilesPerTrigger", "1")
    .config("cleanSource", "delete")
    .appName("Spark Structured Streaming DF 5")
    .getOrCreate()

  def read_inv_json() = {
    val raw_inv_df = spark.readStream
      .format("json")
      .load("src/main/resources/invoices")
//    raw_inv_df.printSchema()

    val explode_inv_df = raw_inv_df.selectExpr("InvoiceNumber","CreatedTime", "StoreID", "PosID", "CustomerType",
      "DeliveryAddress.City", "DeliveryAddress.AddressLine", "DeliveryAddress.PinCode", "DeliveryAddress.State",
      "TaxableAmount", "TotalAmount", "explode(InvoiceLineItems) as LineItem")

//      explode_inv_df.printSchema()

    val flattened_inv_df = explode_inv_df.withColumn("ItemCode", expr("LineItem.ItemCode"))
      .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
      .withColumn("ItemQty", expr("LineItem.ItemQty"))
      .withColumn("TotalValue", expr("LineItem.TotalValue"))
      .drop(col("LineItem"))

//      flattened_inv_df.printSchema()

    flattened_inv_df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    read_inv_json()
  }

}
