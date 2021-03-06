package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Departments_JSON_DF_1 {
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

// define schema for departments.json
  val dept_json_schema = StructType(Array(
    StructField("department_id", IntegerType),
    StructField("department_name", StringType),
    StructField("location_id", IntegerType)
))

  def readfromkafka() = {
    val dept_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "departments")
      .option("startingOffsets", "earliest")
      .load()

    val dept_kafka = dept_df.select(from_json(col("value").cast("String"), dept_json_schema).alias("departments"))
    val target_df = dept_kafka.selectExpr("departments.department_id", "departments.department_name",
    "departments.location_id")


    target_df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    readfromkafka()
  }
}
