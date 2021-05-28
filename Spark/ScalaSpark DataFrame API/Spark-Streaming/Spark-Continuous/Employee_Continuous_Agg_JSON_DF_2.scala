package Spark_Continuous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object Employee_Continuous_Agg_JSON_DF_2 {
  println("Employee_Continuous_Agg_JSON_DF_2")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Employee_Continuous_Agg_JSON_DF_2")
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
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), employees_json_schema).as("employees"))
      .selectExpr("employees.createdtime as createdtime", "employees.employee_id as employee_id", "employees.f_name as f_name", "employees.l_name as l_name",
        "employees.manager_id as manager_id", "employees.department_id as department_id", "employees.salary as salary",
        "employees.hire_dt as hire_dt", "employees.commission as commission")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))

    emp_df.printSchema()

    val salary_agg = emp_df.withWatermark("createdtime", "60 minute")
      .groupBy(window(col("createdtime"), "15 minute"), col("department_id"))
      .agg(
        mean(col("salary")).as("avg_salary")
      )
      .selectExpr("window.start as StartTime", "window.end as EndTime", "department_id", "avg_salary")

    salary_agg.printSchema()

    salary_agg.writeStream
      .format("console")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "chk-point-dir-3")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
