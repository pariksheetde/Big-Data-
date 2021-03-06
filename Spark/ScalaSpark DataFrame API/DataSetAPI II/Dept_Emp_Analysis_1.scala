package DataSetAPI

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Dept_Emp_Analysis_1 {
  println("Departments - Employees Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Department -Employees Analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //  define case class for dept
  case class dept_cc(dept_id: Integer, dept_name: String, loc_id: Integer)

  //  read the dept datafile from the location
  val dept_df: Dataset[Row] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv("D:/Code/DataSet/SparkDataSet/departments.csv")

  //  define case class for emp
  case class emp_cc(Employee_ID: Integer, First_Name: String, Last_Name: String, Email: String, Phone_Number: String, Hire_Date: String,
                    Job_ID: String, Salary: Integer, Commission_pct: Double, Manager_ID: Integer, Department_ID: Integer)

  //  read the emp datafile from the location
  val emp_df: Dataset[Row] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv("D:/Code/DataSet/SparkDataSet/employees.csv")

  //  read the dept case class

  import spark.implicits._

  val sel_dept: Dataset[dept_cc] = dept_df.select("Dept_ID", "Dept_Name", "Loc_ID")
    .as[dept_cc]

  //  read the emp case class
  //  import spark.implicits._
  //  val sel_emp : Dataset[emp_cc] = emp_df.select("Employee_ID", "First_Name", "Last_Name", "Email", "Phone_Number", "Hire_Date", "Job_ID",
  //    "Salary", "Commission_Pct", "Manager_ID", "Department_ID").as[emp_cc]

  //  inner join sel_dept & sel_emp df
  //  define join expression

  def compute() = {
    val Dept_ID_Renamed = dept_df.withColumnRenamed("Department_ID", "Dept_ID")
    val dept_emp_join_expr = Dept_ID_Renamed.col("Dept_ID") === emp_df.col("Department_ID")
    val dept_emp_join_df = Dept_ID_Renamed.join(emp_df, dept_emp_join_expr, "inner")
      .selectExpr("Employee_ID as Emp_ID", "Dept_ID", "Dept_Name", "Loc_ID", "First_Name as F_Name",
        "Last_Name as L_Name", "Email", "Phone_Number", "Hire_Date as Hire_Dt", "Job_ID", "Commission_Pct as Commission")
    dept_emp_join_df.show(false)
    //
    println(s"Records Effected: ${dept_emp_join_df.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
