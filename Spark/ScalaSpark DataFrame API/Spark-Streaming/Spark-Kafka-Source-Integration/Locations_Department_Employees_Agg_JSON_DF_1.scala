package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Locations_Department_Employees_Agg_JSON_DF_1 {
  println("Spark Integration with Kafka 2")
  println("Reading Locations.json from socket and write the output to Kafka")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 2")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

//  define schema for locations.json
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

  def read_from_kafka() = {

    val loc_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe", "locations")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("String"), locations_json_schema).alias("locations"))
      .selectExpr("locations.location_id", "locations.location_name")
      .dropDuplicates()

    val dept_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "departments")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("String"), departments_json_schema).alias("departments"))
      .selectExpr("departments.department_id", "departments.department_name", "departments.location_id")
      .dropDuplicates()

    val emp_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers","localhost:9092")
      .option("subscribe", "employees")
      .option("startingOffsets", "earliest")
      .load()
      .select(from_json(col("value").cast("String"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime", "employees.employee_id", "employees.f_name", "employees.l_name",
        "employees.salary", "employees.department_id")
      .dropDuplicates()

//  join operation between locations.json & departments.json
    val departments_location_id_renamed = dept_df.withColumnRenamed("location_id", "loc_id")
    val loc_dept_join_cond = loc_df.col("location_id") === departments_location_id_renamed.col("loc_id")
    val loc_dept_join = loc_df.join(departments_location_id_renamed, loc_dept_join_cond, "inner")

//  join operation between employees.json & loc_dept
    val employees_department_id_renamed = emp_df.withColumnRenamed("department_id", "dept_id")
    val emp_loc_dept_join_cond = loc_dept_join.col("department_id") === employees_department_id_renamed.col("dept_id")
    val emp_loc_dept = loc_dept_join.join(employees_department_id_renamed, emp_loc_dept_join_cond, "inner")
      .selectExpr("createdtime", "employee_id", "location_id", "dept_id", "department_name", "location_name", "f_name", "l_name", "salary")

   emp_loc_dept.printSchema()

//  calculate average salary within each department_id
    val agg_avg_salary = emp_df.withWatermark("createdtime", "60 minute")
      .groupBy(col("department_id"), window(col("createdtime"), "30 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      ).selectExpr("window.start as StartTime", "window.end as EndTime", "department_id", "avg_salary")

//    calculate the details whose salary is greater than average salary within each department_id
    val target_join_cond = emp_loc_dept.col("dept_id") === agg_avg_salary.col("department_id")
    val target_df = emp_loc_dept.join(agg_avg_salary, target_join_cond, "inner")
      .selectExpr("createdtime", "StartTime", "EndTime", "department_id", "location_id", "department_name", "location_name", "f_name", "l_name", "salary", "avg_salary")
      .where("salary > avg_salary")
      .withColumn("Diff", col("salary") - col("avg_salary"))

    target_df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
    spark.stop()
  }


  def main(args: Array[String]): Unit = {
    read_from_kafka()
  }
}
