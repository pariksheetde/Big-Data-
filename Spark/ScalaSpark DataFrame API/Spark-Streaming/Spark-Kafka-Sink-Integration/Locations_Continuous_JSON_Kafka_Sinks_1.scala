package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object Locations_Continuous_JSON_Kafka_Sinks_1 {
  println("Spark Integration with Kafka 1")
  println("Reading Locations.json from socket and write the output to Kafka")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 1")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  //  define schema for locations.json
  val locations_json_schema = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))


  def compute() = {
    // read locations.json from socket
    val loc_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), locations_json_schema).alias("locations"))
      .selectExpr("locations.location_id as location_id",
        "locations.location_name as location_name")

    val kafka_df = loc_df.select(col("location_name").as("key"),
      to_json(struct(col("location_id"),
        col("location_name"))).cast("String").alias("value"))

    kafka_df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "chk-point-dir")
      .start()
      .awaitTermination()
    spark.stop()
  }


  def main(args: Array[String]): Unit = {
    compute()
  }
}
