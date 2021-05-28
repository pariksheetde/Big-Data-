package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger

object Locations_Departments_MicroBatch_JSON_Kafka_Sink_DF_1 {
  println("Spark Integration with Kafka 1")
  println("Reading from socket and write the output to Kafka")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 1")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
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

  //  read locations.json from path

  def compute() = {
    val loc_df = spark.readStream
      .format("json")
      .load("src/main/resources/locations")

    //  read departments.json for path
    val dept_df = spark.readStream
      .format("json")
      .load("src/main/resources/departments")

    //  join operation between locations.json & departments.json
    val dept_location_id_renamed = dept_df.withColumnRenamed("location_id", "loc_id")
    val loc_dept_join_cond = loc_df.col("location_id") === dept_location_id_renamed.col("loc_id")
    val loc_dept_join = loc_df.join(dept_location_id_renamed, loc_dept_join_cond, "inner")
    loc_dept_join.printSchema()

    //  transform the data in json file format
    val target_df = loc_dept_join.select(col("location_name").as("key"),
      to_json(struct(col("location_id"), col("location_name"),
        col("department_id"), col("department_name"))).cast("String").alias("value")
    )

    //   transform the data in list format
    //    val target_df = loc_dept_join.select(col("location_name").as("key"),
    //      struct(col("location_id"), col("location_name"),
    //        col("department_id"), col("department_name")).cast("String").alias("value")
    //    )

    target_df.writeStream
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
