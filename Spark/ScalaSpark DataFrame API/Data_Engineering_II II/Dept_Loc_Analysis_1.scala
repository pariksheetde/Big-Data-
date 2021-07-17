package Data_Engineering_II

import Data_Engineering_II.Dept_Emp_Analysis_1.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, mean, min, round, sum}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object Dept_Loc_Analysis_1 extends App {
  println("Employees Analysis on Departments")

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
  val cln_dept_df = dept_df.selectExpr("DEPT_ID as DeptID", "DEPT_NAME as Dept_Nm", "LOC_ID as LocID")


  //  read the departments datafile from the location
  val loc_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/Code/DataSet/SparkDataSet/locations.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()
  //  convert the upper case column to lower case
  val cln_loc_df = loc_df.selectExpr("LOCATION_ID as LocID", "STREET_ADDRESS as Street", "POSTAL_CODE as Zip", "City as City",
  "STATE_PROVINCE as State", "COUNTRY_ID as CountryID")

//  join expression between cln_dept_df and cln_emp_df
  val loc_dept_join_expr = cln_loc_df.col("LocID") === cln_dept_df.col("LocID")


//  define the inner join
  val loc_dept_join_df = cln_loc_df.join(cln_dept_df, loc_dept_join_expr, "inner")
  val sel_loc_dept_df = loc_dept_join_df.drop(cln_dept_df.col("LocID"))

  sel_loc_dept_df.selectExpr("LocID", "DeptID", "State", "City", "CountryID")
    .show(false)

  spark.stop()
}
