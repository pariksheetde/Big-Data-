package Data_Engineering_IV

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object Window_Aggregations_1 extends App {
  println("Employee using Seq, Row, StructType")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Employee using StructType")
    .getOrCreate()


  val data = Seq(
    Row("Sales", 1, 5000),
    Row("Personnel", 2, 3900),
    Row("Sales", 3, 5500),
    Row("Sales", 4, 7500),
    Row("Personnel", 5, 3500),
    Row("Developer", 7, 4200),
    Row("Developer", 8, 6000),
    Row("Developer", 9, 4500),
    Row("Developer", 10, 5200),
    Row("Developer", 11, 5200))

  val schema = StructType(List(
    StructField("Dept", StringType),
    StructField("Emp_ID", IntegerType),
    StructField("Salary", IntegerType)
  ))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)

  import spark.implicits._
  val sal_dept = df.select($"Emp_ID", $"Dept", $"Salary")
    .withColumn("Max_Salary", max($"Salary").over(Window.partitionBy($"Dept").orderBy($"Dept".asc,$"Salary".desc)))
    .withColumn("Rank", row_number().over(Window.partitionBy($"Dept").orderBy($"Max_Salary".desc)))
    .show(false)
  spark.stop()
}
