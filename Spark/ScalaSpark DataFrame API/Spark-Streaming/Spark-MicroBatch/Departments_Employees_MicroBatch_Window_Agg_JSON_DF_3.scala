package Spark_MicroBatch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Departments_Employees_MicroBatch_Window_Agg_JSON_DF_3 {
  println("Departments_Employees_MicroBatch_Window_Agg 3")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Departments_Employees_MicroBatch_Window_Agg 3")
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
    val dept_df = spark.readStream
      .format("json")
      .schema(departments_json_schema)
      .load("src/main/resources/departments")

    val emp_df = spark.readStream
      .format("json")
      .schema(employees_json_schema)
      .load("src/main/resources/employees")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumnRenamed("department_id", "dept_id")

    val dept_emp_join_cond = dept_df.col("department_id") === emp_df.col("dept_id")
    val dept_emp_join = dept_df.join(emp_df, dept_emp_join_cond,"inner")

    val agg_avg_salary = dept_emp_join.withWatermark("createdtime", "60 minute")
      .groupBy(col("department_id"), window(col("createdtime"), "30 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      )
      .selectExpr("window.start as StartTime", "window.end as EndTime", "department_id", "avg_salary")

    val target_df_join_cond = agg_avg_salary.col("department_id") === dept_emp_join.col("dept_id")
    val target_df = agg_avg_salary.join(dept_emp_join, target_df_join_cond, "inner")

    val output_df = target_df.selectExpr("StartTime", "EndTime", "employee_id", "dept_id",
      "department_name", "f_name", "l_name", "salary", "avg_salary")
      .where(col("salary") > col("avg_salary"))
      .withColumn("Diff", col("salary") - col("avg_salary"))

    output_df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir-3")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .start()
      .awaitTermination()
  spark.stop()

  }
  def main(args: Array[String]): Unit = {
    compute()
  }
}
