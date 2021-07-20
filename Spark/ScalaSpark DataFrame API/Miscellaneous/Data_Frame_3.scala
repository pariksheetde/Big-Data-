package Miscellaneous

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object Data_Frame_3 {
  println("Data Frame using Seq, Row, StructType")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Data Frame II using StructType")
    .getOrCreate()

  val data = Seq(
    Row("Monica", "Bellucci", "Data Engineering Manager" ,4510000, "25-01-17"),
    Row("Kate", "Beckinsale", "Solution Architect I" ,3010000, "5-12-15"),
    Row("Peter", "Parker", "Solution Architect II" ,3750000, "7-09-99"),
    Row("John", "Carter", "Associate Lead" ,2815000, "8-07-2017"),
    Row("Alexender", "King", "Project Manager" ,4510000, "12-10-11"),
  )

  val schema = StructType(List(
    StructField("F_Name", StringType),
    StructField("L_Name", StringType),
    StructField("Designation", StringType),
    StructField("Salary", IntegerType),
    StructField("Joining_Date", StringType)
  ))

  val df = spark.createDataFrame(
    spark.sparkContext.parallelize(data),schema)

  df.printSchema()
  df.show(false)
  println(s"Number of rows in DF ${df.count()}")

  def compute() = {
    import spark.implicits._
    val date_cln = df.select("F_Name", "L_Name", "Designation", "Salary", "Joining_Date")
      .withColumn("DD_of_Joining", when(substring(col("Joining_Date"), 1, 2) contains ("-"), concat(lit("0"), substring(col("Joining_Date"), 0, 1)))
        otherwise (substring(col("Joining_Date"), 1, 2)))
      .withColumn("Split", split(col("Joining_Date"), "-"))
      .withColumn("Month_of_Joining", $"Split"(1))
      .withColumn("Joining_Year",
        when(substring(col("Joining_Date"), -2, 2) < 20, substring(col("Joining_Date"), -2, 2) + 2000)
          otherwise (substring(col("Joining_Date"), 2, 2) + 1900))
      .withColumn("Year_of_Joining", col("Joining_Year").cast(IntegerType))
      .drop("Joining_Date", "Split", "Joining_Year")
    date_cln.show(false)

    val emp_rpt = date_cln.select("F_Name", "L_Name", "Designation", "DD_of_Joining", "Month_of_Joining", "Year_of_Joining")
      .withColumn("Hire_Date", to_date(expr("concat(DD_of_Joining, '/', Month_of_Joining, '/', Year_of_Joining)"), "d/M/y"))
    emp_rpt.show(false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}