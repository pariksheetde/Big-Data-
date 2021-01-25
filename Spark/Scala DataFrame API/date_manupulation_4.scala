package Bank

import Bank.date_manupulation_2.{columns, data, spark}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, concat, dayofmonth, expr, month, to_date, to_timestamp, year}
import org.apache.spark.sql.types.DateType
import spark.implicits.StringToColumn

object date_manupulation_4 extends App{
  println("Changing Data Format")

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("Changing Data Format")
    .getOrCreate()

  val columns = Seq("Date","Month", "Year")
  val data = Seq(("14", "09", "2000"),
    ("28", "02", "1999"))
  val df = spark.createDataFrame(data).toDF(columns:_*)

  //  below code concats date, month, year
  val res_1 = df.select(col("Date"), col("Month"), col("Year"),
    expr("to_date(concat(Date, Month, Year),'ddMMyyyy') as Date_Of_Birth"))

  //  below code concats date, month, year
  //  val res_2 = df.selectExpr("Date", "Month", "Year",
  //  "to_date(concat(Date, Month, Year),'ddMMyyyy') as Date_of_Birth").show()

  import spark.implicits.StringToColumn
  val res_3 = df.select($"Date", $"Month", $"Year", to_date(concat($"Date", $"Month", $"Year"),"ddMMyyyy").as("Date_Of_Termination")).show()
}
