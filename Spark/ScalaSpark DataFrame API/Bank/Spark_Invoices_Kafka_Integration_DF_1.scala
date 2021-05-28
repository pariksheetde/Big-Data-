package Spark_Kafka_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object Spark_Invoices_Kafka_Integration_DF_1 {
  println("Spark Integration with Kafka 1")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("departments_employees_Continuous_Agg_JSON_DF_1")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
//    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()


//  define schema for invoice
  val inv_json_schema = StructType(List(
    StructField("InvoiceNumber", StringType),
    StructField("CreatedTime", LongType),
    StructField("StoreID", StringType),
    StructField("PosID", StringType),
    StructField("CashierID", StringType),
    StructField("CustomerType", StringType),
    StructField("CustomerCardNo", StringType),
    StructField("TotalAmount", DoubleType),
    StructField("NumberOfItems", IntegerType),
    StructField("PaymentMethod", StringType),
    StructField("CGST", DoubleType),
    StructField("SGST", DoubleType),
    StructField("CESS", DoubleType),
    StructField("DeliveryType", StringType),
    StructField("DeliveryAddress", StructType(List(
      StructField("AddressLine", StringType),
      StructField("City", StringType),
      StructField("State", StringType),
      StructField("PinCode", StringType),
      StructField("ContactNumber", StringType)
    ))),
    StructField("InvoiceLineItems", ArrayType(StructType(List(
      StructField("ItemCode", StringType),
      StructField("ItemDescription", StringType),
      StructField("ItemPrice", DoubleType),
      StructField("ItemQty", IntegerType),
      StructField("TotalValue", DoubleType)
    )))),
  ))

    def read_from_kafka() = {
      val inv_df = spark.readStream
        .format("json")
        .schema(inv_json_schema)
        .load("src/main/resources/invoices")

      val explode_inv_DF = inv_df.selectExpr("InvoiceNumber", "StoreID", "CustomerType", "CustomerCardNo", "TotalAmount", "NumberOfItems",
      "DeliveryAddress.State", "DeliveryAddress.City", "DeliveryAddress.PinCode", "explode(InvoiceLineItems) as LineItem")
//      explodeDF.show(10, false)

      val flattened_inv_df = explode_inv_DF.withColumn("ItemCode", expr("LineItem.ItemCode"))
        .withColumn("ItemDescription", expr("LineItem.ItemDescription"))
        .withColumn("ItemPrice", expr("LineItem.ItemPrice"))
        .withColumn("ItemQty", expr("LineItem.ItemQty"))
        .withColumn("TotalValue", expr("LineItem.TotalValue"))
        .drop("LineItem")

      flattened_inv_df.writeStream
        .format("console")
        .outputMode("append")
        .option("checkpointLocation", "chk-point-dir-5")
        .option("truncate", "false")
        .trigger(Trigger.ProcessingTime("15 seconds"))
        .start()
        .awaitTermination()
      spark.stop()


    }

  def main(args: Array[String]): Unit = {
    read_from_kafka()
  }
}
