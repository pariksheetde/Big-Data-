package Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object Departments_Employees_Data_Analysis_1 {
  println("Accessing Oracle DB")
  println("Departments Employees Data Analysis 1")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Accessing Oracle DB")
    .getOrCreate()

//  Accessing Employees Table from Hr schema
  val emp_df = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:Hr/Hr@localhost:1521/prod")
    .option("dbtable", "hr.employees")
    .option("user", "Hr")
    .option("password", "Hr")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()

//  Accessing Employees Table from Hr schema
  val dept_df = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:Hr/Hr@localhost:1521/prod")
    .option("dbtable", "hr.departments")
    .option("user", "Hr")
    .option("password", "Hr")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()

  def compute() = {
    //  perform join between Employees & Departments
    val emp_department_id_renamed = emp_df.withColumnRenamed("department_id", "dept_id")
    val dept_emp_join_cond = dept_df.col("department_id") === emp_department_id_renamed.col("dept_id")
    val dept_emp_join = dept_df.join(emp_department_id_renamed, dept_emp_join_cond, "inner")
      .selectExpr("employee_id as empID", "dept_id", "department_name", "first_name as f_name", "last_name as l_name", "phone_number as contact",
        "hire_date as hire_dt", "job_id", "salary")
    dept_emp_join.show()
    dept_emp_join.printSchema()
    println(s"Records Effected: ${dept_emp_join.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
