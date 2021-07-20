package Oracle

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._


object Employees_Data_Analysis_1 {
  println("Accessing Oracle DB")
  println("Accessing Hr.Employees Table")


  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Accessing Oracle DB")
    .getOrCreate()

  val emp_df = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:Hr/Hr@localhost:1521/prod")
    .option("dbtable", "hr.employees")
    .option("user", "Hr")
    .option("password", "Hr")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()

  def compute() = {
    emp_df.printSchema()
    emp_df.show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
