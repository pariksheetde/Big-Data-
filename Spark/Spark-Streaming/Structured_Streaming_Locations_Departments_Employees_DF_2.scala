package Spark_Structured_Streaming

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.streaming.Trigger

object Structured_Streaming_Locations_Departments_Employees_DF_2 {
  println("Spark Structured Streaming Locations-Departments-Employees DF 2")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName("Spark Structured Streaming Locations-Departments-Employees DF 2")
    .getOrCreate()

//  define schema for locations.json
  val location_schema = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))

//  read the locations.json
  val locations_df = spark.readStream
    .format("json")
    .schema(location_schema)
    .load("src/main/resources/locations")

//  define schema for departments.json
  val departments_schema = StructType(Array(
    StructField("department_id", IntegerType),
    StructField("department_name", StringType),
    StructField("location_id", IntegerType)
  ))

//  read the departments.json
  val department_df = spark.readStream
    .format("json")
    .schema(departments_schema)
    .load("src/main/resources/departments")

//  define schema for employees
  val employees_schema = StructType(Array(
    StructField("employee_id", IntegerType),
    StructField("f_name", StringType),
    StructField("l_name", StringType),
    StructField("manager_id", IntegerType),
    StructField("department_id", IntegerType),
    StructField("salary", DoubleType),
    StructField("hire_dt", StringType),
    StructField("commission", DoubleType)
  ))

//  read the employees.json from socket
  val employees_df = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 1409)
    .load()
    .select(from_json(col("value"), employees_schema).as("employees"))
    .selectExpr("employees.employee_id as employee_id", "employees.f_name as f_name", "employees.l_name as l_name",
    "employees.manager_id as manager_id", "employees.department_id as department_id", "employees.salary as salary",
    "employees.hire_dt as hire_dt", "employees.commission as commission")


  def loc_dept_join() = {

    val loc_dept_join_cond = locations_df.col("location_id") === department_df.col("location_id")
    val loc_dept = locations_df.join(department_df, loc_dept_join_cond, "inner")

    val emp_loc_dept_join_cond = loc_dept.col("department_id") === employees_df.col("department_id")
    val emp_loc_dept = loc_dept.join(employees_df, emp_loc_dept_join_cond, "inner")

    val final_df = emp_loc_dept.selectExpr("employee_id as emp_id", "f_name", "l_name", "salary",
      "location_name", "department_name", "hire_dt", "manager_id")
      .withColumn("Date", substring(col("hire_dt"),1,2))
      .withColumn("Month", substring(col("hire_dt"),4,2))
      .withColumn("Year",
        when(substring(col("hire_dt"), -2, 2) <= 20, substring(col("hire_dt"), -2, 2) + 2000)
          otherwise(substring(col("hire_dt"),-2, 2) + 1900))
      .drop(col("hire_dt"))

    val output = final_df.select(col("emp_id"), col("f_name"), col("l_name"),
      col("salary"), col("location_name"), col("department_name"), col("manager_id"),
      col("Date"), col("Month"), col("Year"))
      .withColumn("Hire_Date", expr("Date").cast(IntegerType))
      .withColumn("Hire_Month", expr("Month").cast(IntegerType))
      .withColumn("Hire_Year", expr("Year").cast(IntegerType))
      .drop(col("Year"))

    output.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }
  def main(args: Array[String]): Unit = {
    loc_dept_join()
  }
}
