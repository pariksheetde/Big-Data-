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

object Dept_Emp_Analysis_5 extends App {
  println("Departments - Employees Analysis 5 using Spark bucket join")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .enableHiveSupport()
    .getOrCreate()

  val read_dept_df = spark.read.table("DEPT_EMP_db.Departments")
  val read_emp_df = spark.read.table("DEPT_EMP_db.Employees")

  val Dept_ID_Renamed = read_dept_df.withColumnRenamed("Department_ID", "Dept_ID")

  //  define join expression
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)
  val dept_emp_join_expr = Dept_ID_Renamed.col("Dept_ID") === read_emp_df.col("Department_ID")

  //  perform the join logic and select only the required columns
  import org.apache.spark.sql.functions.broadcast
  val dept_emp_join_df = Dept_ID_Renamed.join(read_emp_df, dept_emp_join_expr, "left")
    .withColumn("Commission_Pct", expr("coalesce(Commission_Pct, 1)"))
    .selectExpr("Employee_ID as Emp_ID", "Dept_ID", "Dept_Name as Dept_Nm", "Loc_ID as Loc_ID", "First_Name as F_Name",
      "Last_Name as L_Name", "Email", "Phone_Number", "Hire_Date as Hire_Dt", "Job_ID", "Commission_Pct as Commission")
  dept_emp_join_df.show(false)

  spark.stop()
}