package Spark_Continuous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, from_json, to_timestamp, window}
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object Location_Department_Employees_Continuous_JSON_Agg_DF_1 {
  println("Location_Department_Employees_Continuous_JSON_Agg_DF_1")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Location_Department_Employees_Continuous_JSON_Agg_DF_1")
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
//  read locations.json from socket
    val locations_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), location_json_schema).alias("locations"))
      .selectExpr("locations.location_id as location_id", "locations.location_name as location_name")

//  read departments.json from socket
    val department_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 2302)
      .load()
      .select(from_json(col("value"), departments_json_schema).alias("departments"))
      .selectExpr("departments.department_id as department_id", "departments.department_name as department_name", "departments.location_id as loc_id")


//  read employees.json from socket
    val employee_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1612)
      .load()
      .select(from_json(col("value"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime as createdtime", "employees.employee_id as employee_id", "employees.f_name as f_name", "employees.l_name as l_name",
        "employees.manager_id as manager_id", "employees.department_id as department_id",
        "employees.salary as salary", "employees.hire_dt as hire_dt", "employees.commission as commission")
//      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))
      .withColumnRenamed("department_id", "dept_id")

//    define join condition between locations & departments
    val loc_dept_join_cond = locations_df.col("location_id") === department_df.col("loc_id")
    val loc_dept_join = locations_df.join(department_df, loc_dept_join_cond, "inner")
      .drop("loc_id")

    val emp_loc_dept_cond = employee_df.col("dept_id") === loc_dept_join.col("department_id")
    val emp_loc_dept_join = employee_df.join(loc_dept_join, emp_loc_dept_cond, "inner")
      .selectExpr("employee_id", "location_id", "department_id", "dept_id", "f_name", "l_name", "location_name", "hire_dt", "salary")

    println("Employees - Locations - Employees Join Operation")
    emp_loc_dept_join.printSchema()

//  calculate average salary within each department
    val dept_wise_avg_salary = employee_df.withWatermark("createdtime", "30 minute")
      .groupBy(col("dept_id"), window(col("createdtime"), "60 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      ).selectExpr("window.start as StartTime", "window.end as EndTime", "dept_id", "avg_salary")

//  calculate those employees whose salary is greater than average salary within their respective departments

    val dept_column_renamed = employee_df.withColumnRenamed("dept_id", "department_id")
    val emp_join_cond = dept_wise_avg_salary.col("dept_id") === dept_column_renamed.col("department_id")
    val emp_salary_agg = dept_wise_avg_salary.join(dept_column_renamed, emp_join_cond, "inner")
      .selectExpr("StartTime", "EndTime", "employee_id", "department_id as dept_id", "f_name", "l_name", "hire_dt", "salary", "avg_salary")
    emp_salary_agg.printSchema()

    val final_df_join_cond = emp_salary_agg.col("dept_id") === loc_dept_join.col("department_id")
    val final_df = emp_salary_agg.join(loc_dept_join, final_df_join_cond, "inner")
      .selectExpr("StartTime", "EndTime", "employee_id", "location_id", "dept_id",
        "location_name", "department_name", "f_name", "l_name", "salary", "avg_salary")
      .where(col("salary") > col("avg_salary"))
      .withColumn("Diff", col("salary") - col("avg_salary"))

    println("Final Output to writestream")
    final_df.printSchema()

    final_df.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "chk-point-dir-4")
      .start()
      .awaitTermination()

    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
