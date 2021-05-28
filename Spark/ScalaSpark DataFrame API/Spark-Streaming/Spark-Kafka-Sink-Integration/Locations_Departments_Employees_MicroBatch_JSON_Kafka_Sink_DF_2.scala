package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, from_json, struct, substring, to_json, to_timestamp, when, window}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.streaming.Trigger

object Locations_Departments_Employees_MicroBatch_JSON_Kafka_Sink_DF_2 {
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

    val loc_df = spark.readStream
      .format("json")
      .load("src/main/resources/locations")

    val dept_df = spark.readStream
      .format("json")
      .load("src/main/resources/departments")

    val emp_df = spark.readStream
      .format("json")
      .load("src/main/resources/employees")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))

    //  define join condition between locations and departments
    val departments_location_id_renamed = dept_df.withColumnRenamed("location_id", "loc_id")
    val loc_dept_join_cond = loc_df.col("location_id") === departments_location_id_renamed.col("loc_id")
    val loc_dept_join = loc_df.join(departments_location_id_renamed, loc_dept_join_cond, "inner")

    //  define join operation between employees and loc_dept_join
    val employees_department_id_renamed = emp_df.withColumnRenamed("department_id", "dept_id")
    val emp_loc_dept_join_cond = employees_department_id_renamed.col("dept_id") === loc_dept_join.col("department_id")
    val emp_loc_dept_emp = employees_department_id_renamed.join(loc_dept_join, emp_loc_dept_join_cond, "inner")
      .selectExpr("createdtime", "employee_id", "department_id", "location_id", "f_name", "l_name", "location_name", "department_name", "hire_dt", "salary")

    //  calculate average salary for each department
    val agg_avg_salary = emp_df.withWatermark("createdtime", "30 minute")
      .groupBy(col("department_id"), window(col("createdtime"), "60 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      ).selectExpr("window.start as StartTime", "window.end as EndTime", "department_id", "avg_salary")

    //  calculate the employees, locations, departments whose salary is greater than average salary within their respective departments

    val agg_avg_salary_department_id_renamed = agg_avg_salary.withColumnRenamed("department_id", "dept_id")
    val target_df_join = emp_loc_dept_emp.col("department_id") === agg_avg_salary_department_id_renamed.col("dept_id")
    val target_df = emp_loc_dept_emp.join(agg_avg_salary_department_id_renamed, target_df_join, "inner")
      .withColumn("diff", col("salary") - col("avg_salary"))
      .withColumn("date", substring(col("hire_dt"), 1, 2))
      .withColumn("month", substring(col("hire_dt"), 4, 2))
      .withColumn("year",
        when(substring(col("hire_dt"), -2, 2) <= 20, substring(col("hire_dt"), -2, 2) + 2000)
          otherwise (substring(col("hire_dt"), 2, 2) + 1900))
      .withColumn("date", col("date").cast("Int"))
      .withColumn("month", col("month").cast("Int"))
      .withColumn("year", col("year").cast("Int"))

    //    target_df.printSchema()
    val kafka_df = target_df.select(col("location_name").alias("key"),
      to_json(struct(col("createdtime"), col("employee_id"), col("location_id"), col("department_id"),
        col("location_name"), col("department_name"), col("hire_dt"), col("salary"),
        col("avg_salary"), col("diff"), col("date"),
        col("month"), col("year"))).cast("String").alias("value"))

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
