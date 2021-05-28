package Spark_MicroBatch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Employees_MicroBatch_Agg_JSON_DF_1 {
  println("Employees_MicroBatch_Window_Agg 4")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Employees_MicroBatch_Window_Agg 4")
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

  def compute() = {
    val emp_df = spark.readStream
      .format("json")
      .schema(employees_json_schema)
      .load("src/main/resources/employees")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumnRenamed("department_id", "dept_id")

//  calculate average salary within each department
    val avg_salary_agg = emp_df.withWatermark("createdtime", "60 minute")
      .groupBy(col("dept_id"), window(col("createdtime"), "30 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      )
      .selectExpr("window.start as StartTime", "window.end as EndTime", "dept_id", "avg_salary")

//  select individual employee
    val ind_emp_df = emp_df.selectExpr("employee_id", "f_name", "l_name", "salary", "dept_id as department_id")

//  compute employees whose salary is greater than average salary within their respective department
    val join_cond = ind_emp_df.col("department_id") === avg_salary_agg.col("dept_id")
    val emp_salary = ind_emp_df.join(avg_salary_agg, join_cond, "inner")
    val output_df = emp_salary.selectExpr("StartTime", "EndTime", "department_id", "f_name", "l_name", "salary", "avg_salary")
      .filter(col("salary") > col("avg_salary"))
      .withColumn("Diff", col("salary") - col("avg_salary"))

    output_df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir-4")
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
