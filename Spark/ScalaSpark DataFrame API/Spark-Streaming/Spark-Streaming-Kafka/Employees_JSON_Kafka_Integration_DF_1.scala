package Spark_Streaming_Kafka

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, from_json, struct, to_json, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._


object Employees_JSON_Kafka_Integration_DF_1 {
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

  def emp_kafka_int() = {
    val emp_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employees")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("String"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime as createdtime", "employees.employee_id as employee_id", "employees.f_name as f_name", "employees.l_name as l_name",
        "employees.manager_id as manager_id", "employees.department_id as department_id",
        "employees.salary as salary", "employees.hire_dt as hire_dt", "employees.commission as commission")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))

    val emp_kafka_df = emp_df.select(col("f_name").alias("key"),
      to_json(struct(col("employee_id"), col("f_name"))).cast("String").alias("value")
    )
      .dropDuplicates()

    emp_kafka_df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .option("topic", "employees")
      .option("checkpointLocation", "chk-point-dir")
      .start()
      .awaitTermination()
    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    emp_kafka_int()
  }
}
