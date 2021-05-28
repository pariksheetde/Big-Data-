package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger

object Locations_Departments_MicroBatch_JSON_Kafka_Sink_DF_2 {
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

  //  define schema for locations.json
  val locations_schema_json = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))

  //  define schema for departments.json
  val departments_schema_json = StructType(Array(
    StructField("department_id", StringType),
    StructField("department_name", IntegerType),
    StructField("location_id", IntegerType)
  ))

  def compute() = {

    //  read locations.json from path
    val loc_df = spark.readStream
      .format("json")
      .load("src/main/resources/locations")

    //  read departments.json from path
    val dept_df = spark.readStream
      .format("json")
      .load("src/main/resources/departments")

    loc_df.printSchema()
    dept_df.printSchema()

    //  define join condition between locations.json & departments.json
    val department_location_id_renamed = dept_df.withColumnRenamed("location_id", "loc_id")
    val loc_dept_join_cond = loc_df.col("location_id") === department_location_id_renamed.col("loc_id")
    val loc_dept_join = loc_df.join(department_location_id_renamed, loc_dept_join_cond, "inner")
      .filter("location_name = 'Berlin'")

    //  transform the data frame to kafka format
    val kafka_df = loc_dept_join.select(col("location_name").as("key"),
      to_json(struct(col("location_id"), col("department_id"),
        col("location_name"),
        col("department_name"))).cast("String").alias("value")
    )

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
