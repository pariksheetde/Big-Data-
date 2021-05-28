package Spark_Kafka_Sink_Integration

import com.datastax.spark.connector.types.InetType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, from_json, struct, substring, to_json, to_timestamp, when, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.streaming.Trigger

object Locations_Departments_Employees_Continuous_JSON_Kafka_Sink_DF_2 {
  println("Spark Integration with Kafka 2")
  println("Reading from socket and write the output to Kafka")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 2")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 2)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

//  define schema for locations.json
val department_json_schema = StructType(Array(
  StructField("department_id", IntegerType),
  StructField("department_name", StringType),
  StructField("location_id", IntegerType)
))

  // define schema for locations.json
  val locations_schema_json = StructType(Array(
    StructField("location_id", IntegerType),
    StructField("location_name", StringType)
  ))

  // define schema for departments.json
  val departments_json_schema = StructType(Array(
    StructField("department_id", IntegerType),
    StructField("department_name", StringType),
    StructField("location_id", IntegerType)
  ))

  // define schema for employees.json
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

    val loc_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), locations_schema_json).as("locations"))
      .selectExpr("locations.location_id as location_id",
        "locations.location_name as location_name")

    val dept_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 2302)
      .load()
      .select(from_json(col("value"), departments_json_schema).alias("departments"))
      .selectExpr("departments.department_id as department_id",
        "departments.department_name as department_name",
        "departments.location_id as location_id")

    val emp_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1612)
      .load()
      .select(from_json(col("value"), employees_json_schema).alias("employees"))
      .selectExpr( "employees.createdtime as createdtime",
        "employees.employee_id as employee_id",
        "employees.f_name as f_name",
        "employees.l_name as l_name",
        "employees.manager_id as manager_id",
        "employees.department_id as department_id",
        "employees.salary as salary",
        "employees.hire_dt as hire_dt",
        "employees.commission as commission")

// define join operation between locations & departments
    val departments_location_id_renamed = dept_df.withColumnRenamed("location_id", "loc_id")
    val loc_dept_join_cond = loc_df.col("location_id") === departments_location_id_renamed.col("loc_id")
    val loc_dept_join = loc_df.join(departments_location_id_renamed, loc_dept_join_cond, "inner")

//  define join operation employees & loc_dept
    val employees_department_id_renamed = emp_df.withColumnRenamed("department_id", "dept_id")
    val emp_loc_dept_join_cond = loc_dept_join.col("department_id") === employees_department_id_renamed.col("dept_id")
    val emp_loc_dept = loc_dept_join.join(employees_department_id_renamed, emp_loc_dept_join_cond, "inner")
      .withColumn("date", substring(col("createdtime"), 1,2))
      .withColumn("month", substring(col("createdtime"),4,2))
      .withColumn("year",
        when(substring(col("hire_dt"), -2, 2) <= 20, substring(col("hire_dt"), -2, 2) + 2000)
          otherwise (substring(col("hire_dt"), 2, 2) + 1900))
      .withColumn("date", col("date").cast("Int"))
      .withColumn("month", col("month").cast("Int"))
      .withColumn("year", col("year").cast("Int"))
      .drop("hire_dt")
    emp_loc_dept.printSchema()

    val kafka_df = emp_loc_dept.select(col("location_name").alias("key"),
      to_json(struct(col("createdtime"), col("employee_id"), col("loc_id"), col("department_id"),
        col("f_name"), col("l_name"), col("location_name"), col("department_name"),
        col("salary"), col("date"), col("month"),
        col("year"))).cast("String").alias("value"))

    kafka_df.printSchema()

    kafka_df.writeStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .outputMode("append")
      .trigger(Trigger.ProcessingTime("15 seconds"))
      .option("topic", "rockthejvm")
      .option("checkpointLocation", "chk-point-dir")
      .start()
      .awaitTermination()
    spark.stop()


  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
