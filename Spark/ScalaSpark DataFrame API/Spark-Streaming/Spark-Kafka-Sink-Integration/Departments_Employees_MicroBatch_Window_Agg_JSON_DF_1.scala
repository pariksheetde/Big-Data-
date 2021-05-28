package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

object Departments_Employees_MicroBatch_Window_Agg_JSON_DF_1 {
  println("Spark Integration with Kafka 1")
  println("Reading Locations.json from socket and write the output to Kafka")

  val spark = SparkSession.builder()
    .appName("Spark Integration with Kafka 1")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()

  def compute() = {

    //  read departments.json from path
    val dept_df = spark.readStream
      .format("json")
      .load("src/main/resources/departments")

    //  read employees.json from path
    val emp_df = spark.readStream
      .format("json")
      .load("src/main/resources/employees")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))

    //  join operation between departments.json & employees.json
    val employees_department_id_renamed = emp_df.withColumnRenamed("department_id", "dept_id")
    val dept_emp_join_cond = dept_df.col("department_id") === employees_department_id_renamed.col("dept_id")
    val dept_emp_join = dept_df.join(employees_department_id_renamed, dept_emp_join_cond, "inner")
      .selectExpr("createdtime", "employee_id", "department_id", "department_name", "dept_id", "location_id", "f_name", "l_name", "hire_dt", "salary")

    //  calculate average salary within each departments
    val agg_salary_df = emp_df.withWatermark("createdtime", "30 minute")
      .groupBy(col("department_id"), window(col("createdtime"), "60 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      )

    //  join between dept_emp_join with agg_salary_df to get the employees details whose salary is greater than average salary within respective departments
    val salary_gt_avg_salary_join_cond = dept_emp_join.col("dept_id") === agg_salary_df.col("department_id")
    val salary_gt_avg_salary = dept_emp_join.join(agg_salary_df, salary_gt_avg_salary_join_cond, "inner")
      .selectExpr("createdtime", "employee_id", "location_id", "dept_id", "department_name", "f_name", "l_name", "hire_dt", "salary", "avg_salary")
      .where(col("salary") > col("avg_salary"))
      .withColumn("Diff", col("salary") - col("avg_salary"))

    val kafka_df = salary_gt_avg_salary.select(col("department_name").alias("key"),
      to_json(struct(col("createdtime"), col("employee_id"), col("location_id"), col("dept_id"),
        col("f_name"), col("l_name"), col("salary"), col("avg_salary"),
        col("Diff"))).cast("String").alias("value")
    )

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
