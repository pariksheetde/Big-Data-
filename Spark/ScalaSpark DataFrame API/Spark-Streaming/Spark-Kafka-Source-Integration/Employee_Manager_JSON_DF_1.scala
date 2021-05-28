package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Employee_Manager_JSON_DF_1 {
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
    StructField("createdtime", TimestampType),
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
    val emp_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employees")
      .option("startingOffsets", "earliest")
      .load()

    val emp_kafka_df = emp_df.select(from_json(col("value").cast("String"), employees_json_schema).alias("employees"))
      .selectExpr( "employees.f_name as emp_f_name", "employees.l_name as emp_l_name", "employees.manager_id", "employees.salary as emp_salary")

    val manager_kafka_df = emp_df.select(from_json(col("value").cast("String"),employees_json_schema).alias("manager"))
      .selectExpr("manager.employee_id", "manager.f_name as man_f_name", "manager.l_name as man_l_name",  "manager.salary as man_salary")

    println("Employees's Schema")
    emp_kafka_df.printSchema()
    println("Manager's Schema")
    manager_kafka_df.printSchema()

    val emp_man_join_cond = emp_kafka_df.col("manager_id") === manager_kafka_df.col("employee_id")
    val emp_manager_df = emp_kafka_df.join(manager_kafka_df, emp_man_join_cond, "inner")
      .selectExpr("employee_id", "emp_f_name", "emp_l_name", "man_f_name", "man_l_name", "emp_salary", "man_salary")

    println("Final Kafka Target Schema")
    emp_manager_df.printSchema()

    emp_manager_df.writeStream
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
