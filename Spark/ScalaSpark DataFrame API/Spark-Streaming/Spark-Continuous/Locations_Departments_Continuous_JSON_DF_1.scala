package Spark_Continuous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Locations_Departments_Continuous_JSON_DF_1 {
  println("Locations_Departments 1")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Locations_Departments 1")
    .master("local[3]")
    .config("spark.streaming.stopGracefullyOnShutdown", "true")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.sql.streaming.schemaInference", "true")
    .getOrCreate()


  def compute() = {
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

    // read locations.json from socket
    val loc_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 1409)
      .load()
      .select(from_json(col("value"), location_json_schema).alias("locations"))
      .selectExpr("locations.location_id as location_id", "locations.location_name as location_name")

    loc_df.printSchema()

    //  read departments.json from socket
    val dept_df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 2302)
      .load()
      .select(from_json(col("value"), departments_json_schema).alias("departments"))
      .selectExpr("departments.department_id as department_id", "departments.department_name as department_name",
        "departments.location_id as loc_id")

    dept_df.printSchema()

    val loc_dept_join_cond = loc_df.col("location_id") === dept_df.col("loc_id")
    val loc_dept_join = loc_df.join(dept_df, loc_dept_join_cond, "inner")
    val output_df = loc_dept_join.selectExpr("location_id", "department_id", "location_name", "department_name")

    output_df.writeStream
      .format("console")
      .outputMode("append")
      .option("truncate", "false")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .option("checkpointLocation", "chk-point-dir-5")
      .start()
      .awaitTermination()

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
