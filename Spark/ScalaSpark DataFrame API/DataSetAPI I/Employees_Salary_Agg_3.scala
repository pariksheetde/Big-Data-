package DataSet_API

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._

object Employees_Salary_Agg_3 {
  println("Employees Salary Aggregation Analysis 3")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  //  define case class for emp
  case class emp_cc(Employee_ID: Integer, First_Name: String, Last_Name: String, Email: String, Phone_Number: String, Hire_Date: String,
                    Job_ID: String, Salary: Integer, Commission_pct: Double, Manager_ID:Integer, Department_ID: Integer)

  //  read the emp datafile from the location
  val emp_df : Dataset[Row] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv("D:/DataSet/DataSet/SparkDataSet/employees.csv")

  //  read the emp case class
  import spark.implicits._
  val sel_emp : Dataset[emp_cc] = emp_df.select("Employee_ID", "First_Name", "Last_Name", "Email", "Phone_Number", "Hire_Date", "Job_ID",
    "Salary", "Commission_Pct", "Manager_ID", "Department_ID").as[emp_cc]

  //  count number of records in emp DF
  val cnt_rec = sel_emp.count()
  println("Number of records : " + cnt_rec)

  def compute() = {

    //  SQl to perform aggregation on average salary group by department_id
    val agg_dept = sel_emp.createOrReplaceTempView("Dept_Avg_Salary")
    spark.sql(
      """select
        |department_id as dept_id,
        |round(avg(salary),2) as avg_salary,
        |sum(salary * coalesce(Commission_Pct, 1)) as CTC
        |from
        |Dept_Avg_Salary
        |group by dept_id
        |order by CTC desc
        |""".stripMargin).show(false)

    val grp_dept = sel_emp.groupBy("Department_ID")
      .agg(avg("Salary").alias("avg_salary"),
        sum(expr("salary * coalesce(commission_pct, 1)")).as("CTC"))
      .sort(expr("CTC desc"))
      .show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}