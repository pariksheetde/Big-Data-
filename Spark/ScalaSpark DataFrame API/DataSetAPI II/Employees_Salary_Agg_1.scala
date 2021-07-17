package DataSetAPI

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object Employees_Salary_Agg_1 {
  println("Employees Salary Aggregation Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

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

  //  read the emp case class

  import spark.implicits._

  val sel_emp: Dataset[emp_cc] = emp_df.select("Employee_ID", "First_Name", "Last_Name", "Email", "Phone_Number", "Hire_Date", "Job_ID",
    "Salary", "Commission_Pct", "Manager_ID", "Department_ID").as[emp_cc]

  //  count number of records in emp DF
  val cnt_rec = sel_emp.count()
  println("Number of records : " + cnt_rec)

  def compute() = {
    //  count sum, avg, max, min of salary, count of commission
    val agg_sal = sel_emp.select(sum("Salary").as("Sum_Salary"),
      avg("Salary").as("Avg_Salary"),
      mean("Salary").alias("Avg_Salary"),
      min("Salary").alias("Min_Salary"),
      count("Salary").alias("Cnt_Salary"),
      countDistinct("Employee_ID").as("Cnt_Unique_Employees"),
      countDistinct("Department_ID").as("Cnt_Unique_Dept_ID"),
      count("Commission_Pct").as("Cnt_Non_Null_Commission")).show()

    sel_emp.printSchema()
    sel_emp.show(false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
