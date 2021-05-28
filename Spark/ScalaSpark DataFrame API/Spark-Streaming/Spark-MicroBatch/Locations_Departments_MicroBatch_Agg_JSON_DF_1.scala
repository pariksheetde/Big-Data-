package Spark_MicroBatch

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.streaming.Trigger

object Locations_Departments_MicroBatch_Agg_JSON_DF_1 {
  println("Locations_Departments_MicroBatch_Agg_JSON_DF_1")
  println("Reading from socket and write the output to console")

  val spark = SparkSession.builder()
    .appName("Locations_Departments_MicroBatch_Agg_JSON_DF_1")
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


  //  define schema for locations.json
  val departments_json_schema = StructType(Array(
    StructField("department_id", IntegerType),
    StructField("department_name", StringType),
    StructField("location_id", StringType),
  ))


  def compute() = {

    //  read locations.json from path
    val loc_df = spark.read
      .format("json")
      .schema(locations_json_schema)
      .load("src/main/resources/locations")

    //  read locations.json from path
    val dept_df = spark.read
      .format("json")
      .schema(departments_json_schema)
      .load("src/main/resources/departments")
      .withColumnRenamed("location_id", "loc_id")

//  join operation between locations and departments

    val loc_dept_join_cond = loc_df.col("location_id") === dept_df.col("loc_id")
    val loc_dept_join = loc_df.join(dept_df, loc_dept_join_cond, "inner")


//  compute number of departments in each location_id
    val dept_cnt = dept_df.groupBy(col("loc_id"))
      .agg(
        count(col("department_id")).alias("cnt_of_departments")
      )

//  compute location_id, location_name and count of departments in each location
    val join_cond = loc_dept_join.col("loc_id") === dept_cnt.col("loc_id")
    val loc_agg_loc_name = loc_dept_join.join(dept_cnt, join_cond, "inner")
      .selectExpr("location_id", "location_name", "cnt_of_departments")
      .show(false)
    spark.stop()

  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
