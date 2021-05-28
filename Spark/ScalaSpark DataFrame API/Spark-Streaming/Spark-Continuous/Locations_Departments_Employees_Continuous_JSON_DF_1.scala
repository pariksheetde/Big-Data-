package Spark_Continuous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, substring, when}
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object Locations_Departments_Employees_Continuous_JSON_DF_1 {
  println("Spark Integration with Kafka 1")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 3")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

//  define schema for locations.json
  val location_json_schema = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))

//  define schema for departments.json
  val department_json_schema = StructType(Array(
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


//  create schema for locations.json
  def compute() = {
    val loc_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), location_json_schema).alias("locations"))
      .selectExpr("locations.location_id as location_id", "locations.location_name as location_name")

//  read departments.json from socket
    val dept_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 2302)
      .load()
      .select(from_json(col("value"), department_json_schema).alias("departments"))
      .selectExpr("departments.department_id as department_id", "departments.department_name as department_name", "departments.location_id as loc_id")


//  read employees.json from file
    val employees_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1612)
      .load()
      .select(from_json(col("value"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime as createdtime",
                         "employees.employee_id as employee_id", "employees.f_name as f_name",
                         "employees.l_name as l_name", "employees.manager_id as manager_id",
                         "employees.department_id as dept_id", "employees.salary as salary",
                         "employees.hire_dt as hire_dt", "employees.commission as commission")


//  join operation between locations & departments
    val loc_dept_join_cond = loc_df.col("location_id") === dept_df.col("loc_id")
    val loc_dept_join = loc_df.join(dept_df, loc_dept_join_cond, "inner")
      .drop("loc_id")

//  join operation between employees & join-result set
    val emp_join_cond = employees_df.col("dept_id") === loc_dept_join.col("department_id")
    val emp_join = employees_df.join(loc_dept_join, emp_join_cond, "inner")
      .selectExpr("employee_id", "location_id", "manager_id", "f_name", "l_name", "department_name", "salary", "hire_dt", "commission")
      .withColumn("date", substring(col("hire_dt"), 1, 2))
      .withColumn("month", substring(col("hire_dt"), 4, 2))
      .withColumn("year",
        when(substring(col("hire_dt"), -2, 2) <= 20, substring(col("hire_dt"), -2, 2) + 2000)
          otherwise(substring(col("hire_dt"), 2, 2) + 1900))
      .withColumn("year", col("year").cast(IntegerType))
      .drop(col("hire_dt"))

    emp_join.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("7 seconds"))
      .option("checkpointLocation", "chk-point_dir-6")
      .start()
      .awaitTermination()

  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
