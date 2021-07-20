package Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf

object Locations_Departments_Employees_Data_Analysis_3 {
  println("Accessing Oracle DB")
  println("Querying Locations, Departments, Employees Table")

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

  val emp_df = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:Hr/Hr@localhost:1521/prod")
    .option("dbtable", "hr.employees")
    .option("user", "Hr")
    .option("password", "Hr")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()

  def compute() = {
    //  join operation between locations & departments table
    val dept_location_id_renamed_df = dept_df.withColumnRenamed("location_id", "loc_id")
    val loc_dept_df = loc_df.join(dept_location_id_renamed_df, loc_df.col("location_id") === dept_location_id_renamed_df.col("loc_id"), "inner")
      .selectExpr("loc_id", "department_id as dept_id", "department_name as dept_name", "street_address as address", "city", "state_province as state", "country_id", "manager_id")

    //  join operation between loc_dept & employees
    val loc_dept_emp_df = loc_dept_df.join(emp_df, loc_dept_df.col("dept_id") === emp_df.col("department_id"))
      .select(
        col("employee_id"), col("dept_id"), col("loc_id"),
        concat_ws(" ", col("first_name"), col("last_name")).alias("name"),
        lower(concat(col("email"), lit("@gmail.com"))).alias("email")
        ,col("hire_date"), col("address"), col("city"), col("state"),
        dayofmonth(col("hire_date")).alias("day"),
        month(col("hire_date")).alias("month"), year(col("hire_date")).alias("year")
      )
      .drop(col("hire_date"))
    loc_dept_emp_df.show(truncate = false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
