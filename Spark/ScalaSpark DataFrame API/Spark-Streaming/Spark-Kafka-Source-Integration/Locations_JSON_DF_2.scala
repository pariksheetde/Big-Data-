package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Locations_JSON_DF_2 {
  println("Spark Integration with Kafka 2")
  println("Reading Locations.json from socket and write the output to Kafka")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 2")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  // define schema for locations.json
  val locations_json_schema = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))

  def read_from_kafka() = {
    val loc_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "locations")
      .option("startingOffsets", "earliest")
      .load()

    val loc_trans_df = loc_df.select(from_json(col("value").cast("String"), locations_json_schema).alias("value"))
    val target_df = loc_trans_df.selectExpr("value.location_id as location_id", "value.location_name as location_name")
      .dropDuplicates()
      .where("location_name = 'Berlin'")

    target_df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    read_from_kafka()
  }
}
