package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Locations_Departments_JSON_DF_1 {
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

//  define schema for locations.json
  val locations_json_schema = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))

//  define schema for departments.json
  val departments_json_schema = StructType(Array(
    StructField("department_id", IntegerType),
    StructField("department_name", StringType),
    StructField("location_id", IntegerType)
  ))

  def read_from_kafka() = {
    val loc_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "locations")
      .load()

    val loc_kafka = loc_df.select(from_json(col("value").cast("String"), locations_json_schema).alias("locations"))
      .selectExpr("locations.location_id", "locations.location_name")

    val dept_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("startingOffsets", "earliest")
      .option("subscribe", "departments")
      .load()

    val dept_kafka = dept_df.select(from_json(col("value").cast("String"), departments_json_schema).alias("departments"))
      .selectExpr("departments.department_id", "departments.department_name", "departments.location_id")

    val dept_kafka_location_id_renamed = dept_kafka.withColumnRenamed("location_id", "loc_id")
    val loc_dept_join_cond = dept_kafka_location_id_renamed.col("loc_id") === loc_kafka.col("location_id")
    val loc_dept_join = loc_kafka.join(dept_kafka_location_id_renamed, loc_dept_join_cond, "inner")
      .where("location_name = 'London'")

    println("Kafka Target Schema")
    loc_dept_join.printSchema()

    loc_dept_join.writeStream
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
    read_from_kafka()
  }
}
