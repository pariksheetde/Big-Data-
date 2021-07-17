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

object Employees_Data_Analysis_2 {
  println("Employees Data Analysis 2")

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Employees Data Analysis 2")
    .getOrCreate()

  //  define schema for employees.json
  val emp_json_schema = new StructType()
    .add("ins_dt", DateType, nullable = true)
    .add("emp_id", IntegerType, nullable = true)
    .add("f_name", StringType, nullable = true)
    .add("l_name", StringType, nullable = true)
    .add("manager_id", IntegerType, nullable = true)
    .add("dept_id", IntegerType, nullable = true)
    .add("salary", DoubleType, nullable = true)
    .add("hire_dt", StringType, nullable = true)
    .add("commission", DoubleType, nullable = true)

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
    val agg_salary_df = emp_df.groupBy("dept_id")
      .agg(
        mean("salary").alias("avg_salary"),
        sum("salary").alias("sum_salary"),
        max("salary").as("max_salary"),
        min("salary").as("min_salary")
      )
      .withColumn("sum_salary", col("sum_salary").cast("Decimal(10,2)"))

    agg_salary_df.printSchema()
    agg_salary_df.show(10, false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
