package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger

object Departments_Employees_MicroBatch_JSON_Kafka_Sink_2 {
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

  def compute() = {
    // read departments.json from path
    val dept_df = spark.readStream
      .format("json")
      .load("src/main/resources/departments")

    //  read employees.json from path
    val emp_df = spark.readStream
      .format("json")
      .load("src/main/resources/employees")

    // join operation between departments.json & employees.json
    val departments_location_id_renamed = dept_df.withColumnRenamed("department_id", "dept_id")
    val dept_emp_join_cond = emp_df.col("department_id") === departments_location_id_renamed.col("dept_id")
    val dept_loc_join = emp_df.join(departments_location_id_renamed, dept_emp_join_cond, "inner")

    val trans_df = dept_loc_join.selectExpr("department_id", "location_id", "department_name", "employee_id", "f_name", "l_name", "hire_dt", "salary")
      .withColumn("date", substring(col("hire_dt"), 1, 2))
      .withColumn("month", substring(col("hire_dt"), 4, 2))
      .withColumn("year",
        when(substring(col("hire_dt"), -2, 2) <= 20, substring(col("hire_dt"), -2, 2) + 2000)
          otherwise (substring(col("hire_dt"), 2, 2) + 1900))
      .withColumn("date", col("date").cast("Int"))
      .withColumn("month", col("month").cast("Int"))
      .withColumn("year", col("year").cast("Int"))
      .where("date >= 15")
      .drop("hire_dt")

    val kafka_df = trans_df.select(col("department_name").alias("key"),
      to_json(struct(col("employee_id"), col("location_id"), col("department_id"), col("department_name"),
        col("f_name"), col("l_name"), col("salary"), col("date"),
        col("month"), col("year"))).cast("String").alias("value")
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
