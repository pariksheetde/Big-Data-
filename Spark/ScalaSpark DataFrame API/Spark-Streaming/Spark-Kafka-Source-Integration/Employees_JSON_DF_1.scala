package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Employees_JSON_DF_1 {
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

  //  define schema for employees.json
  val employees_json_schema = StructType(Array(
    StructField("createdtime", StringType),
    StructField("employee_id", IntegerType),
    StructField("f_name", StringType),
    StructField("l_name", StringType),
    StructField("manager_id", IntegerType),
    StructField("department_id", IntegerType),
    StructField("salary", DoubleType),
    StructField("hire_dt", StringType),
    StructField("commission", DoubleType)
  ))

  def read_from_kafka() = {
    val employees_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employees")
      .option("startingOffsets", "earliest")
      .load()

    employees_df.printSchema()
    val employees_transfomed_target = employees_df.select(from_json(col("value").cast("String"), employees_json_schema).as("employees"))
    employees_transfomed_target.printSchema()

    val kafka_target = employees_transfomed_target.selectExpr("employees.createdtime", "employees.employee_id", "employees.f_name", "employees.l_name",
      "employees.manager_id", "employees.department_id", "employees.salary")
      .where("salary >= 2500000")
      .where("manager_id is null")

    kafka_target.writeStream
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
