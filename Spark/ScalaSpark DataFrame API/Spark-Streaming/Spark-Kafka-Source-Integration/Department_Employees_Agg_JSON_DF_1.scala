package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Department_Employees_Agg_JSON_DF_1 {
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

//  define schema for departments.json
  val dept_json_schema = StructType(Array(
    StructField("department_id", IntegerType),
    StructField("department_name", StringType),
    StructField("location_id", IntegerType)
  ))

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
    val dept_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "departments")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("String"), dept_json_schema).alias("departments"))
      .selectExpr("departments.department_id", "departments.department_name", "departments.location_id")
//      .dropDuplicates()

    val emp_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employees")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("String"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime", "employees.employee_id", "employees.f_name", "employees.l_name",
        "employees.salary", "employees.department_id")
      .dropDuplicates()

//  define join condition between departments.json & employees.json
    val employees_department_id_renamed = emp_df.withColumnRenamed("department_id", "dept_id")
    val dept_emp_join_cond = dept_df.col("department_id") === employees_department_id_renamed.col("dept_id")
    val dept_emp_join = dept_df.join(employees_department_id_renamed,  dept_emp_join_cond,"inner")
      .selectExpr("createdtime", "employee_id", "dept_id", "f_name", "l_name", "salary")

    dept_emp_join.printSchema()

//  calculate average salary within each department_id
    val agg_salary_df = emp_df.withWatermark("createdtime", "60 minute")
      .groupBy(col("department_id"), window(col("createdtime"), "30 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      ).selectExpr("window.start as StartTime", "window.end as EndTime", "department_id", "avg_salary")

    agg_salary_df.printSchema()

    val target_df = dept_emp_join.join(agg_salary_df, dept_emp_join.col("dept_id") === agg_salary_df.col("department_id"), "inner")
      .where("salary > avg_salary")
      .withColumn("Diff", col("salary") - col("avg_salary"))
      .selectExpr("createdtime", "StartTime", "EndTime", "employee_id", "dept_id", "f_name", "l_name", "salary", "avg_salary", "Diff")


    target_df.writeStream
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
