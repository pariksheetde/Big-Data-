package DataSetAPI

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

import java.sql.Date

object Employees_Data_Analysis_1 {
  println("Employees Data Analysis 1")

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Employees Data Analysis 1")
    .getOrCreate()

//  define schema for employees.json
  val emp_json_schema = StructType(Array(
    StructField("ins_dt", DateType),
    StructField("emp_id", IntegerType),
    StructField("f_name", StringType),
    StructField("l_name", StringType),
    StructField("manager_id", IntegerType),
    StructField("dept_id", IntegerType),
    StructField("salary", DoubleType),
    StructField("hire_dt", StringType),
    StructField("commission", DoubleType)
  ))

//  define case class for Employees
  case class Employees (ins_dt: Date, emp_id: Int, f_name: String, l_name: String, manager_id: Int, dept_id:Int,
                        salary: Double, hire_dt: String, commission: Double)

//  read employee.json
  import spark.implicits._
  val emp_df  = spark.read
    .schema(emp_json_schema)
    .json("D:/Code/DataSet/SparkStreamingDataSet/employees")
    .as[Employees]

  def compute() = {
    val filtered_emp_df = emp_df.selectExpr(("ins_dt"), ("emp_id"), ("f_name"), ("l_name"), ("hire_dt"), ("salary"))
      .withColumn("date", substring(col("hire_dt"), 1, 2))
      .withColumn("month", substring(col("hire_dt"),4,3))
      .withColumn("year",
        when(substring(col("hire_dt"), -2, 2) <= 20, substring(col("hire_dt"), -2, 2) + 2000)
          otherwise (substring(col("hire_dt"), 2, 2) + 1900))
      .drop("hire_dt")
      .withColumn("date", col("date").cast("Int"))
      .withColumn("year", col("year").cast("Int"))
    filtered_emp_df.show(10, false)

    filtered_emp_df.printSchema()
  }
  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
