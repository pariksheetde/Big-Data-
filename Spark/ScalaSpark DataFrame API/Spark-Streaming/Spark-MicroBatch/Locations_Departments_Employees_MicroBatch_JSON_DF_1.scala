package Spark_MicroBatch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger


object Locations_Departments_Employees_MicroBatch_JSON_DF_1 {
  println("Locations_Departments_Employees_MicroBatch 1")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Locations_Departments_Employees_MicroBatch 1")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()


// define schema for locations.json
  val locations_json_schema = StructType(Array(
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
    val loc_df = spark.readStream
      .format("json")
      .schema(locations_json_schema)
      .load("src/main/resources/locations")
      .withColumnRenamed("location_id", "loc_id")

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

    val loc_dept_join_cond = loc_df.col("loc_id") === dept_df.col("location_id")
    val loc_dept_join = loc_df.join(dept_df, loc_dept_join_cond, "inner")
      .selectExpr("location_id", "loc_id", "department_id", "location_name", "department_name")

    val loc_dept_emp_join_cond = loc_dept_join.col("department_id") === emp_df.col(("dept_id"))
    val loc_dept_emp_join = loc_dept_join.join(emp_df, loc_dept_emp_join_cond, "inner")
      .selectExpr("createdtime", "employee_id", "location_id" ,"department_id", "f_name", "l_name",
        "location_name", "department_name", "salary")

    loc_dept_emp_join.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir-5")
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
