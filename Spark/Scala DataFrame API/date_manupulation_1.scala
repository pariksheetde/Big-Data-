package Bank

import Bank.date_manupulation_2.{dateconversion, df}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.SparkConf

object date_manupulation_1 extends App {
  println("Changing Data Format using StructType")

  def dateconversion(df: DataFrame, fmt: String, fld: String): DataFrame = {
    df.withColumn(fld, to_date(col(fld),fmt))
  }

  val spark:SparkSession = SparkSession.builder()
    .master("local[3]").appName("Date Formatting")
    .getOrCreate()


  import spark.implicits._
  val columns = Seq("ID","EventDate")
  val data = Seq((100, "01/01/2020"), (110, "02/02/2020"), (120, "03/03/2020"))
  val df = spark.createDataFrame(data).toDF(columns:_*)

  val def_schema = StructType(List(
    StructField("ID", IntegerType, true),
    StructField("EventDate", StringType, true)
  )
  )

  //  before conversion from string type to date type
  df.printSchema()
  df.show()

  //  after conversion from string type to date type
  val func = dateconversion(df, "M/d/y", "EventDate")
  func.printSchema()
  func.show(10)

}
