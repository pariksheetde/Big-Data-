package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger

object Locations_Departments_Continuous_JSON_Kafka_Sink_DF_1 {
  println("Spark Integration with Kafka 2")
  println("Reading from socket and write the output to Kafka")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 2")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  def compute() = {

    // define schema for locations.json
    val loc_schema = StructType(Array(
      StructField("location_id", IntegerType),
      StructField("location_name", StringType)
    ))

    //  define schema for departments.json
    val dept_schema = StructType(Array(
      StructField("department_id", IntegerType),
      StructField("department_name", StringType),
      StructField("location_id", IntegerType)
    ))

    //  read locations.json from socket
    val loc_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), loc_schema).alias("locations"))
      .selectExpr("locations.location_id as location_id", "locations.location_name as location_name")

    //  read departments.json from socket
    val dept_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 2302)
      .load()
      .select(from_json(col("value"), dept_schema).alias("departments"))
      .selectExpr("departments.department_id as department_id", "departments.department_name as department_name",
        "departments.location_id as loc_id")

    val loc_dept_join_cond = loc_df.col("location_id") === dept_df.col("loc_id")
    val loc_dept_join = loc_df.join(dept_df, loc_dept_join_cond, "inner")

    // convert into kafka key-value format
    val kafka_target_df = loc_dept_join.select(
      col("location_name").as("key"),
      to_json(struct(col("department_id"), col("location_id"),
        col("department_name"), col("location_name"))).cast("String").alias("value"))
    //, col("department_name"),
    kafka_target_df.printSchema()

    kafka_target_df.writeStream
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
