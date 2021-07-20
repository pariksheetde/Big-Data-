package Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object Departments_Employees_Data_Analysis_2 {
  println("Accessing Oracle DB")
  println("Departments Employees Data Analysis 2")

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
      .selectExpr("employee_id as emp_id", "dept_id", "department_name", "first_name as f_name", "last_name as l_name", "phone_number as contact",
        "hire_date as hire_dt", "job_id", "salary")

    val dept_emp = dept_emp_join.withColumn("date", dayofyear(col("hire_dt")))
      .withColumn("month", month(col("hire_dt")))
      .withColumn("year", year(col("hire_dt")))
      .drop(col("hire_dt"))
    dept_emp.show()
    dept_emp.printSchema()
    println(s"Records Effected: ${dept_emp.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
