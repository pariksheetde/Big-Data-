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

object Dept_Emp_Analysis_2 {
  println("Departments - Employees Analysis 2 using broadcast join")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //  define case class for dept
  case class dept_cc(Dept_ID:Integer, Dept_Name: String, Loc_ID: Integer)

  //  read the dept datafile from the location
  val dept_df : Dataset[Row] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv("D:/DataSet/DataSet/SparkDataSet/departments.csv")

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

  //  read the dept case class
  import spark.implicits._
  val sel_dept : Dataset[dept_cc] = dept_df.select("Dept_ID", "Dept_Name", "Loc_ID")
    .as[dept_cc]

  //  read the emp case class
  import spark.implicits._
  val sel_emp : Dataset[emp_cc] = emp_df.select("Employee_ID", "First_Name", "Last_Name", "Email", "Phone_Number", "Hire_Date", "Job_ID",
    "Salary", "Commission_Pct", "Manager_ID", "Department_ID").as[emp_cc]

  def compute() = {
    //  left join sel_dept & sel_emp df
    val Dept_ID_Renamed = dept_df.withColumnRenamed("Department_ID", "Dept_ID")
    //  define join expression

    //  define join expression
    val dept_emp_join_expr = Dept_ID_Renamed.col("Dept_ID") === emp_df.col("Department_ID")

    //  perform broadcast join on Dept_ID_Renamed: smaller DF
    //  perform the join logic and select only the required columns
    import org.apache.spark.sql.functions.broadcast
    val dept_emp_join_df = broadcast(Dept_ID_Renamed).join(emp_df, dept_emp_join_expr, "left")
      .withColumn("COMMISSION_PCT", expr("coalesce(COMMISSION_PCT, 1)"))
      .selectExpr("Employee_ID as Emp_ID", "Dept_ID", "Dept_Name as Dept_Nm", "Loc_ID as Loc_ID", "First_Name as F_Name",
        "Last_Name as L_Name", "Email", "Phone_Number", "Hire_Date as Hire_Dt", "Job_ID", "Commission_Pct as Commission")

    dept_emp_join_df.show(false)
    println(s"Record count ${dept_emp_join_df.count()}") // 20
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }

}