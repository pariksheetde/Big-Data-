package Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object Employees_Agg_Salary_1 {
  println("Accessing Oracle DB")
  println("Employees Aggregated Salary 1")

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


  def compute() = {
// compute average salary for each department
   val agg_salary_dept = emp_df.groupBy(col("department_id"))
     .agg(
       avg(col("salary")).alias("avg_salary")
     )

//  return employees details
    val emp_ind_df = emp_df.select(
      col("employee_id"), col("department_id"),
      concat_ws(" ", col("first_name") , col("last_name")).alias("name"),
       col("salary")
    ).withColumnRenamed("department_id","dept_id")

    val emp_self_join_cond = agg_salary_dept.col("department_id") === emp_ind_df.col("dept_id")
    val emp_self_join = agg_salary_dept.join(emp_ind_df, emp_self_join_cond, "inner")
      .selectExpr("employee_id as emp_id", "dept_id", "name", "salary", "avg_salary")
      .filter(col("salary") > col("avg_salary"))
      .sort(col("dept_id").asc)

    emp_self_join.show()
    print(s"Records Effected ${emp_self_join.count()}")

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}