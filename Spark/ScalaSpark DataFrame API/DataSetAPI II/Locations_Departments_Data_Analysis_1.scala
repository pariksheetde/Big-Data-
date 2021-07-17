package DataSetAPI

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object Locations_Departments_Data_Analysis_1 {
  println("Locations-Departments Data Analysis 2")

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Locations-Departments Data Analysis 2")
    .getOrCreate()

  //  define schema for locations.json
  val loc_json_schema = StructType(Array(
    StructField("loc_id", IntegerType),
    StructField("loc_name", StringType)
  ))

//  define schema for departments.json
  val dept_json_schema = StructType(Array(
    StructField("dept_id", IntegerType),
    StructField("dept_name", StringType),
    StructField("loc_id", IntegerType)
  ))

  case class Locations(loc_id: Int, loc_name: String)
  case class Departments(dept_id: Int, dept_name: String, loc_id: Int)

  import spark.implicits._
  val loc_df = spark.read
    .schema(loc_json_schema)
    .json("D:/Code/DataSet/SparkStreamingDataSet/locations")
    .as[Locations]

  import spark.implicits._
  val dept_df = spark.read
    .schema(dept_json_schema)
    .json("D:/Code/DataSet/SparkStreamingDataSet/departments")
    .as[Departments]

  def compute() = {

    val dept_loc_id_renamed = dept_df.withColumnRenamed("loc_id", "location_id")
    val loc_dept_join_cond = loc_df.col("loc_id") === dept_loc_id_renamed.col("location_id")
    val loc_dept_join = loc_df.join(dept_loc_id_renamed, loc_dept_join_cond, "inner")
      .selectExpr("loc_id", "dept_id", "loc_name", "dept_name")

//     loc_dept_join.filter(loc_dept_join("dept_name") like "%Spark%") , loc_dept_join.filter(loc_dept_join("dept_name") like "%DBA%"
    val dba_df = loc_dept_join.filter(loc_dept_join("dept_name") like "%DBA%")

    val spark_df = loc_dept_join.where(loc_dept_join("dept_name") like "%PySpark%")

    dba_df.union(spark_df)
      .show(10, false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
