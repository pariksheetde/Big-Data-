package DataSetAPI

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}

object Dept_Emp_Analysis_4 {
  println("Departments - Employees Analysis 4 using Spark bucket join")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .enableHiveSupport()
    //    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  //  define case class for dept
  case class dept_cc(Dept_ID: Integer, Dept_Name: String, Loc_ID: Integer)

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

  def compute() = {
    import spark.implicits._
    val sel_emp: Dataset[emp_cc] = emp_df.select("Employee_ID", "First_Name", "Last_Name", "Email", "Phone_Number", "Hire_Date", "Job_ID",
      "Salary", "Commission_Pct", "Manager_ID", "Department_ID").as[emp_cc]
    //  SAVE THE DEPARTMENTS DF TO HIVE MANAGED TABLE
    spark.sql("CREATE DATABASE IF NOT EXISTS DEPT_EMP_db")
    spark.sql("USE DEPT_EMP_db")
    spark.sql("drop table if exists dept_emp_db.Departments")
    val buck_dept_df = sel_dept.write
      .bucketBy(3, "Dept_ID")
      .mode(SaveMode.Overwrite)
      .saveAsTable("Departments")

    // SAVE THE EMPLOYEES DF TO HIVE MANAGED TABLE
    spark.sql("USE DEPT_EMP_db")
    spark.sql("drop table if exists dept_emp_db.Employees")
    val buck_emp_df = sel_emp.write
      .bucketBy(3, "Department_ID")
      .mode(SaveMode.Overwrite)
      .saveAsTable("Employees")
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
