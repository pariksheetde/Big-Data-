package Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object Locations_Departments_Data_Analysis_2 {
  println("Accessing Oracle DB")
  println("Querying Locations & Departments Table")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Accessing Oracle DB")
    .getOrCreate()

  val loc_df = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:Hr/Hr@localhost:1521/prod")
    .option("dbtable", "hr.locations")
    .option("user", "Hr")
    .option("password", "Hr")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()

  val dept_df = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:Hr/Hr@localhost:1521/prod")
    .option("dbtable", "hr.departments")
    .option("user", "Hr")
    .option("password", "Hr")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()

  def compute() = {
    val dept_location_id_df = dept_df.withColumnRenamed("location_id", "loc_id")
    val loc_dept_df = loc_df.join(dept_location_id_df, loc_df.col("location_id") === dept_location_id_df.col("loc_id"), "inner")
      .selectExpr("loc_id", "department_id as dept_id", "department_name as dept_name",
        "street_address as address", "postal_code as zip", "city as city", "country_id as country_id",
        "state_province as state")
      .where(col("state") like "%Wash%")

    loc_dept_df.show(10, truncate = false)
    println(s"Records Effected: ${loc_dept_df.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
