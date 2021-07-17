package Data_Engineering_II

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}


object Department_DF extends App {
  println("Reading Departments file")

//  create a spark session
  val sparkAppConf = new SparkConf()
  sparkAppConf.set("spark.app.name", "Reading Departments file")
  sparkAppConf.set("spark.master", "local[3]")

  val spark = SparkSession.builder()
    .config(sparkAppConf)
    .getOrCreate()

//  define schema for Dept
  val dept_sch = StructType(List(
    StructField("DEPT_ID", IntegerType, false),
    StructField("DEPT_NAME", StringType),
    StructField("LOC_ID", IntegerType)
  ))

//  read a dept file
  val dept_df = spark.read
    .format("csv")
    .schema(dept_sch)
    .option("header", "true")
    .load("D:/Code/DataSet/SparkDataSet/departments.csv")

  val sel_dept = dept_df.selectExpr("DEPT_ID as Dept_ID", "DEPT_NAME as Dept_Nm",
    "LOC_ID as Loc_ID")

  sel_dept.printSchema()
  sel_dept.show(false)

  spark.stop()
}
