package Spark_Continuous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object Departments_Employees_Continuous_Agg_JSON_DF_1 {
  println("Spark Integration with Kafka 1")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("departments_employees_Continuous_Agg_JSON_DF_1")
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

  def compute() = {
    //  read departments.json from socket
    val dept_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), departments_json_schema).alias("departments"))
      .selectExpr("departments.department_id as department_id", "departments.department_name as department_name", "departments.location_id as location_id")

    //  read employees.json from socket
    val emp_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 2302)
      .load()
      .select(from_json(col("value"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime as createdtime", "employees.employee_id as employee_id", "employees.f_name as f_name", "employees.l_name as l_name",
        "employees.manager_id as manager_id", "employees.department_id as dept_id", "employees.salary as salary",
        "employees.hire_dt as hire_dt", "employees.commission as commission")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))

    val dept_emp_join_cond = dept_df.col("department_id") === emp_df.col("dept_id")
    val dept_emp_join = dept_df.join(emp_df, dept_emp_join_cond, "inner")
    val output_df = dept_emp_join.selectExpr("createdtime", "department_id", "department_name", "salary")

    val agg_salary = output_df.withWatermark("createdtime", "1 minute")
      .groupBy(col("department_id"),
        col("department_name"),
        window(col("createdtime"), "15 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      )
    val expr_df = agg_salary.selectExpr("window.start as StartTime", "window.end as EndTime", "department_id", "department_name", "avg_salary")
    expr_df.printSchema()

    expr_df.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("truncate", "false")
      .option("checkpointLocation", "chk-point-dir-1")
      .start()
      .awaitTermination()

    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
