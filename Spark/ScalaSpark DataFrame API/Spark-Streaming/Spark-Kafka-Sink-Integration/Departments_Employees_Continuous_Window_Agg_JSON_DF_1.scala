package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._
//import org.apache.spark.sql.types.{IntegerType, StructField, StructType}

object Departments_Employees_Continuous_Window_Agg_JSON_DF_1 {
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

  def compute() = {

   val dept_df = spark.readStream
     .format("socket")
     .option("host", "localhost")
     .option("port", 1409)
     .load()
     .select(from_json(col("value"), departments_json_schema).alias("departments"))
     .selectExpr("departments.department_id as department_id", "departments.department_name as department_name", "departments.location_id as location_id")


    val emp_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 2302)
      .load()
      .select(from_json(col("value"), employees_json_schema).alias("employees"))
      .selectExpr( "employees.createdtime as createdtime",
        "employees.employee_id as employee_id",
        "employees.f_name as f_name",
        "employees.l_name as l_name",
        "employees.manager_id as manager_id",
        "employees.department_id as department_id",
        "employees.salary as salary",
        "employees.hire_dt as hire_dt",
        "employees.commission as commission")

//  join operation between department.json and employees.json
    val employees_department_id_renamed = emp_df.withColumnRenamed("department_id", "dept_id")
    val dept_emp_join_cond = employees_department_id_renamed.col("dept_id") === dept_df.col("department_id")
    val dept_emp_join = employees_department_id_renamed.join(dept_df, dept_emp_join_cond, "inner")
      .selectExpr("createdtime", "employee_id", "f_name", "l_name", "salary", "dept_id", "department_id", "department_name")

    println("Departments - Employees schema")
    dept_emp_join.printSchema()

//    compute average salary within each department
    val agg_avg_salary = emp_df.withWatermark("createdtime", "60 minute")
      .groupBy(col("department_id"), window(col("createdtime"), "30 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      ).selectExpr("window.start as StartTime", "window.end as EndTime", "department_id", "avg_salary")

    println("Employee's Average Salary computation")
    agg_avg_salary.printSchema()

//  calculate the salary of those employees whose salary is greater than the average salary within each department_id
    val join_cond = dept_emp_join.col("dept_id") === agg_avg_salary.col("department_id")
    val salary_gt_avg_salary = dept_emp_join.join(agg_avg_salary, join_cond, "inner")
      .selectExpr("createdtime", "StartTime", "EndTime", "employee_id", "dept_id", "f_name", "l_name", "department_name", "salary", "avg_salary")
      .where(col("salary") > col("avg_salary"))
      .withColumn("Diff", col("salary") - col("avg_salary"))

    println("Final column list")
    salary_gt_avg_salary.printSchema()

    val kafka_df = salary_gt_avg_salary.select(col("department_name").alias("key"),
      to_json(struct(col("createdtime"), col("StartTime"), col("EndTime"),
        col("employee_id"), col("dept_id"), col("f_name"), col("l_name"), col("department_name"),
        col("salary"), col("avg_salary"), col("Diff"))).cast("String").alias("value")
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
