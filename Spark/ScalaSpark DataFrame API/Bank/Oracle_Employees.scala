package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object Oracle_Employees extends App {

  println("Accessing Oracle DB")


  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Accessing Oracle DB")
//    .config("spark.jars", "file:/D:/Driver/driver/ojdbc8.jar")
//    .config("spark.executor.extraClassPath", "file:/D:/Driver/driver/ojdbc8.jar")
//    .config("spark.driver.extraClassPath", "file:/D:/Driver/driver/ojdbc8.jar")
    .getOrCreate()

  val qry = "select employee_id from Hr.employees"

  val emp_df = spark.read.format("jdbc")
    .option("url", "jdbc:oracle:thin:Hr/Hr@localhost:1521/orcl")
    .option("dbtable", "hr.employees")
    .option("user", "Hr")
    .option("password", "Hr")
    .option("driver", "oracle.jdbc.driver.OracleDriver")
    .load()
}
