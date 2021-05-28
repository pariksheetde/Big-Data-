package Spark_Continuous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Employee_Continuous_Agg_JSON_DF_1 {
  println("Employee_Continuous_Agg_JSON_DF_1")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Employee_Continuous_Agg_JSON_DF_1")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  //  define schema for employees
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
    //  read employees.json from src/main/resources/employees
    val employee_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime as createdtime", "employees.employee_id as employee_id", "employees.f_name as f_name", "employees.l_name as l_name",
        "employees.manager_id as manager_id", "employees.department_id as department_id",
        "employees.salary as salary", "employees.hire_dt as hire_dt", "employees.commission as commission")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))

    // calculate average salary for employees for each department_id
    val avg_salary_agg = employee_df.withWatermark("createdtime", "60 minute")
      .groupBy(col("department_id"),
        window(col("createdtime"), "15 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      )
    val avg_salary_expr = avg_salary_agg.selectExpr("window.start as StartTime",
      "window.end as EndTime", "department_id", "avg_salary")


    // calculate individual employee details
    val ind_emp_df = employee_df.select(col("employee_id"), col("f_name"), col("l_name"),
      col("department_id"), col("salary"))

    val agg_salary_join_cond = avg_salary_expr.col("department_id") === ind_emp_df.col("department_id")
    val agg_salary_join = avg_salary_expr.join(ind_emp_df, agg_salary_join_cond, "inner")
    val output_df = agg_salary_join.selectExpr("StartTime", "EndTime", "employee_id", "f_name", "l_name", "salary", "avg_salary")
      .filter(col("salary") > col("avg_salary"))
      .withColumn("Diff", col("salary") - col("avg_salary"))
    output_df.printSchema()


    output_df.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .option("checkpointLocation", "chk-point-dir-2")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
