package Spark_Kafka_Sink_Integration

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json, struct, to_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{avg, col, to_timestamp, window}
import org.apache.spark.sql.streaming.Trigger

object Locations_Departments_Employees_MicroBatch_JSON_Kafka_Sink_DF_1 {
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
    //  read locations.json from path
    val loc_df = spark.readStream
      .format("json")
      .load("src/main/resources/locations")

    //  read departments.json from path
    val dept_df = spark.readStream
      .format("json")
      .load("src/main/resources/departments")

    //  read employees.json from path
    val emp_df = spark.readStream
      .format("json")
      .load("src/main/resources/employees")
      .withColumn("createdtime", to_timestamp(col("createdtime"), "yyyy-MM-dd HH:mm:ss"))

    //   define join operation between locations.json & departments.json
    val dept_location_id_renamed = dept_df.withColumnRenamed("location_id", "loc_id")
    val loc_dept_join_cond = loc_df.col("location_id") === dept_location_id_renamed.col("loc_id")
    val loc_dept_join = loc_df.join(dept_location_id_renamed, loc_dept_join_cond, "inner")

    //  define join condition between employees.json and loc_dept_join
    val employees_department_id_renamed = emp_df.withColumnRenamed("department_id", "dept_id")
    val emp_loc_dept_join_cond = employees_department_id_renamed.col("dept_id") === loc_dept_join.col("department_id")
    val emp_loc_dept = employees_department_id_renamed.join(loc_dept_join, emp_loc_dept_join_cond, "inner")
      .selectExpr("createdtime", "employee_id", "location_id", "department_id", "dept_id", "location_name",
        "department_name", "f_name", "l_name", "hire_dt", "salary")

    //  calculate average salary within each department
    val agg_salary = emp_df.withWatermark("createdtime", "60 minute")
      .groupBy(col("department_id"), window(col("createdtime"), "30 minute"))
      .agg(
        avg(col("salary")).alias("avg_salary")
      )

    //  calculate the employees and departments, locations details whose salary is greater than the average salary within their respective departments
    val target_df_join_cond = emp_loc_dept.col("dept_id") === agg_salary.col("department_id")
    val target_df = emp_loc_dept.join(agg_salary, target_df_join_cond, "inner")
      .where(col("salary") > col("avg_salary"))
      .withColumn("Diff", col("salary") - col("avg_salary"))

    val kafka_df = target_df.selectExpr("createdtime", "employee_id", "dept_id", "location_id", "f_name", "l_name",
      "location_name", "hire_dt", "salary", "avg_salary", "Diff")

    val kafka_write = kafka_df.select(col("location_name").alias("key"),
      to_json(struct(col("createdtime"), col("employee_id"), col("dept_id"), col("location_id"),
        col("f_name"), col("l_name"), col("location_name"), col("hire_dt"),
        col("salary"), col("avg_salary"), col("Diff"))).cast("String").alias("value"))

    //    kafka_write.printSchema()
    //    kafka_write.show(10, false)

    kafka_write.writeStream
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
