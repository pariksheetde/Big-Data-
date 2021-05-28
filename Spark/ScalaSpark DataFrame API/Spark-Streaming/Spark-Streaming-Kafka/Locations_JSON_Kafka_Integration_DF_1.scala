package Spark_Streaming_Kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, from_json, struct, to_json, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Locations_JSON_Kafka_Integration_DF_1 {
  println("Departments_Employees_MicroBatch_Agg 2")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Departments_Employees_MicroBatch_Agg 2")
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

  def Kafka_Int() = {

    val loc_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "locations")
      .option("startingOffsets", "latest")
      .load()
      .select(from_json(col("value").cast("String"), locations_json_schema).alias("locations"))
      .selectExpr("locations.location_id", "locations.location_name")

    loc_df.printSchema()

    val kafka_target = loc_df.select(col("location_name").as("key"),
      to_json(struct(col("location_id"), col("location_name"))).cast("String").alias("value"))

    kafka_target.printSchema()

    kafka_target.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .option("topic", "locations")
      .option("checkpointLocation", "chk-point-dir")
      .start()
      .awaitTermination()
    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    Kafka_Int()
  }
}
