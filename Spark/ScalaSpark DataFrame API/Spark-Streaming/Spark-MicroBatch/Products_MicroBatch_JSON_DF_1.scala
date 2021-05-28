package Spark_MicroBatch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Products_MicroBatch_JSON_DF_1 {
  println("Products_MicroBatch_Window_Agg 4")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Employees_MicroBatch_Window_Agg 4")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

//  define schema for products.json
  val prod_json_schema = StructType(Array(
    StructField("ItemCode", IntegerType),
    StructField("ItemDescription", StringType),
    StructField("ItemPrice", StringType),
    StructField("ItemQty", IntegerType)
  ))

  def products() = {
    val prod_df = spark.readStream
      .format("json")
      .schema(prod_json_schema)
      .load("src/main/resources/products")
    prod_df.printSchema()

//  select all the records whose ItemQty > 0
    val item_qty_gt_0 = prod_df.selectExpr("*")
      .filter("ItemQty >= 3")

    item_qty_gt_0.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir-1")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    products()
  }
}
