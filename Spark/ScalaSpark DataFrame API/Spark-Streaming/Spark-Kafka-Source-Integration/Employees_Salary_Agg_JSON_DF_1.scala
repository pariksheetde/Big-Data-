package Spark_Kafka_Source_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types._

object Employees_Salary_Agg_JSON_DF_1 {
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

    val emp_df = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "employees")
      .option("startingOffsets", "earliest")
      .load()

    val ind_emp_df = emp_df.select(from_json(col("value").cast("String"), employees_json_schema).alias("employees"))
      .selectExpr("employees.createdtime", "employees.employee_id", "employees.f_name", "employees.l_name",
        "employees.salary", "employees.department_id")
      .dropDuplicates()

    //  compute average salary within each department
    val salary_avg_agg = emp_df.select(from_json(col("value").cast("String"), employees_json_schema).as("agg_salary"))
      .select("agg_salary.salary", "agg_salary.department_id", "agg_salary.createdtime")
      .withWatermark("createdtime", "60 minute")
      .groupBy(col("department_id"), window(col("createdtime"), "30 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      ).selectExpr("window.start as StartTime", "window.end as EndTime", "department_id", "avg_salary")

    salary_avg_agg.printSchema()

    //  calculate employee details whose salary is greater than average salary within each department
    val employees_department_id_renamed = ind_emp_df.withColumnRenamed("department_id", "dept_id")
    val target_df_join_cond = employees_department_id_renamed.col("dept_id") === salary_avg_agg.col("department_id")
    val target_df = employees_department_id_renamed.join(salary_avg_agg, target_df_join_cond, "inner")
      .where(col("salary") > col("avg_salary"))
      .withColumn("diff", col("salary") - col("avg_salary"))

    target_df.writeStream
      .format("console")
      .outputMode("append")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
      .awaitTermination()
    spark.stop()


  }

  def main(args: Array[String]): Unit = {
    read_from_kafka()
  }
}
