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

object Structured_Streaming_Locations_Departments_Employees_DF_1 {
  println("Spark Structured Streaming Locations-Departments-Employees DF 1")

  val spark = SparkSession.builder()
    .config("spark.master", "local[2]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions",1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .appName("Spark Structured Streaming Locations-Departments-Employees DF 1")
    .getOrCreate()

  //  define schema for locations.json
  val location_schema = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))

  //  define schema for departments.json
  val departments_schema = StructType(Array(
    StructField("department_id", IntegerType),
    StructField("department_name", StringType),
    StructField("location_id", IntegerType)
  ))

//  define schema for employees.json. employees.json is produced at runtime / stream
  val emoloyees_schema = StructType(Array(
    StructField("employee_id", IntegerType),
    StructField("f_name", StringType),
    StructField("l_name", StringType),
    StructField("manager_id", IntegerType),
    StructField("department_id", IntegerType),
    StructField("salary", IntegerType),
    StructField("hire_dt", StringType),
    StructField("commission", IntegerType)
  ))


  val locations_df = spark.readStream
    .format("json")
    .schema(location_schema)
    .load("src/main/resources/locations")

  val departments_df = spark.readStream
    .format("json")
    .schema(departments_schema)
    .load("src/main/resources/departments")

  val employees_DF = spark.readStream
    .format("socket")
    .option("host", "localhost")
    .option("port", 1409)
    .load()
    .select(from_json(col("value"), emoloyees_schema).as("employees"))
    .selectExpr("employees.employee_id as employee_id", "employees.f_name as f_name", "employees.l_name as l_name",
    "employees.manager_id as manager_id", "employees.department_id as department_id", "employees.salary as salary",
    "employees.hire_dt as hire_dt", "employees.commission as commission")

  def loc_dept() = {
    val loc_dept_join_cond = locations_df.col("location_id") === departments_df.col("location_id")
    val loc_dept_join = locations_df.join(departments_df, loc_dept_join_cond, "inner")
      .drop(departments_df.col("location_id"))

    val emp_jon_cond = employees_DF.col("department_id") === loc_dept_join.col("department_id")
    val join_employees = employees_DF.join(loc_dept_join, emp_jon_cond, "inner")

    val final_df = join_employees.selectExpr("employee_id", "f_name", "l_name", "location_id", "department_name",
      "location_name", "salary", "hire_dt", "manager_id", "commission")

    final_df.writeStream
      .format("console")
      .outputMode("append") // append is only supported in stream vs stream join
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    loc_dept()
  }
}
