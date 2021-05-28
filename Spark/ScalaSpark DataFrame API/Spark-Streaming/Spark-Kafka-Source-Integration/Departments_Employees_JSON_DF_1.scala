package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._


object Departments_Employees_JSON_DF_1 {
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
  val departments_json_schema = StructType(Array(
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
      .select(from_json(col("value").cast("String"),departments_json_schema).alias("departments"))
      .selectExpr("departments.department_id", "departments.department_name", "departments.location_id")
      .dropDuplicates()

    val emp_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employees")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("String"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime", "employees.employee_id", "employees.f_name", "employees.l_name",
        "employees.manager_id", "employees.department_id as dept_id", "employees.salary", "employees.hire_dt", "employees.commission")
      .dropDuplicates()

//  join operation between departments & employees
    val dept_emp_join_cond = dept_df.col("department_id") === emp_df.col("dept_id")
    val dept_emp_join = dept_df.join(emp_df, dept_emp_join_cond, "inner")
      .selectExpr("employee_id", "dept_id", "f_name", "l_name", "manager_id", "hire_dt", "salary")
      .withColumn("date", substring(col("hire_dt"), 1, 2))
      .withColumn("month", substring(col("hire_dt"),4,2))
      .withColumn("year",
        when(substring(col("hire_dt"), -2, 2) <= 20, substring(col("hire_dt"), -2, 2) + 2000)
          otherwise (substring(col("hire_dt"), 2, 2) + 1900))
      .withColumn("date", col("date").cast("Int"))
      .withColumn("month", col("month").cast("Int"))
      .withColumn("year", col("year").cast("Int"))
      .drop(col("hire_dt"))

    dept_emp_join.writeStream
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
