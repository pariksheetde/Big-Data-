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

object Employees_Salary_Agg_2 {
  println("Employees Salary Aggregation Analysis 2")

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

  def compute() = {
    //  read the emp case class
    import spark.implicits._
    val sel_emp: Dataset[emp_cc] = emp_df.select("Employee_ID", "First_Name", "Last_Name", "Email", "Phone_Number", "Hire_Date", "Job_ID",
      "Salary", "Commission_Pct", "Manager_ID", "Department_ID").as[emp_cc]

    //  count number of records in emp DF
    val cnt_rec = sel_emp.count()
    println("Number of records : " + cnt_rec + " from source")

    //  count sum, avg, max, min of salary, count of commission
    val agg_emp = sel_emp.selectExpr("count(Employee_ID) as Cnt", "avg(Salary) as Avg_Sal",
      "mean(Salary) as Avg_Salary", "min(Salary) as Min_Sal", "count(salary) as Cnt_Salary",
      "count(distinct Employee_ID) as Cnt_Unique_Employees",
      "count(distinct Department_ID) as Cnt_Unique_Dept_ID",
      "count(Commission_Pct) as Cnt_Non_Null_Commission")

      agg_emp.show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }

}