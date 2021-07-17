package Data_Engineering_II

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, mean, min, round, sum}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object Dept_Emp_Analysis_1 extends App {
  println("Employees Analysis on Departments")
  println("Dept_Emp_Analysis_1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Employees Analysis on Departments")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  //  read the departments datafile from the location
  val dept_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/Code/DataSet/SparkDataSet/Departments.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()

//  convert the upper case column to lower case
  val cln_dept_df = dept_df.selectExpr("Dept_ID as DeptID", "Dept_Name as Dept_Nm",
    "LOC_ID as LocID")

//  cln_dept_df.printSchema()
//  cln_dept_df.show()

  //  read the employees datafile from the location
  val emp_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/Code/DataSet/SparkDataSet/Employees.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()

//  convert the upper case column to lower case
  val cln_emp_df = emp_df.selectExpr("EMPLOYEE_ID as EmpID", "FIRST_NAME as F_Name", "LAST_NAME as L_Name", "EMAIL as Email_ID",
  "PHONE_NUMBER as Contact", "HIRE_DATE as Joining_Date", "JOB_ID as Job_ID", "SALARY as Salary", "COMMISSION_PCT as Commission",
  "MANAGER_ID as Manager_ID", "DEPARTMENT_ID as DeptID")

//  cln_emp_df.printSchema()
//  cln_emp_df.show()

//  rename department_id in cln_emp_df to dept_id
  val cln_emp_dept_id = cln_emp_df.withColumnRenamed("DeptID", "Dept_ID")

//  join expression between dept and emp
  val dept_emp_join_expr = cln_emp_dept_id.col("Dept_ID") === cln_dept_df.col("DeptID")

// write the inner join condition between cln_dept_df & cln_emp_dept_id
  val dept_emp_join_df = cln_dept_df.join(cln_emp_dept_id, dept_emp_join_expr, "inner")
  val dept_emp_df = dept_emp_join_df.selectExpr("EmpID", "Dept_ID" ,"F_Name", "L_Name", "Email_ID", "Joining_Date", "Salary")

  dept_emp_df.show(false)
  println(s"Records Effected: ${dept_emp_df.count()}") // 106

}
