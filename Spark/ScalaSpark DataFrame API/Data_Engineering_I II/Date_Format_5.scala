package Data_Engineering_I

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, column, expr, monotonically_increasing_id, when, _}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._

object Date_Format_4 {
  println("Date Format 2 using StructType")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  val datalist = List(
    ("Monica", "12", "01", "2002"),
    ("Kate", "14", "10", "81"), // 1981
    ("Pamela", "16", "12", "06"), // 2006
    ("Peter", "25", "05", "63"), // 1963
    ("Kylie", "19", "09", "85"), //1985
  )

  val df = spark.createDataFrame(datalist).toDF("Name", "Date", "Month", "Year")

  df.printSchema()
  df.show()

  def compute() = {
    val res_df = df.withColumn("id", monotonically_increasing_id())
      .withColumn("Date", expr("Date").cast(IntegerType))
      .withColumn("Month", expr("Month").cast(IntegerType))
      .withColumn("Year", expr("Year").cast(IntegerType))
      .withColumn("Year",
        when(col("Year") < 21, col("Year") + 2000)
          when(col("Year") < 100, col("Year") + 1900)
          otherwise (col("Year")))
      .withColumn("DOB", to_date(expr("concat(Date, '/', Month, '/', Year)"), "d/M/y"))
      .drop("Date", "Month", "Year") // remove the unwanted columns
      .dropDuplicates("Name", "DOB")
      .sort(expr("DOB desc"))

    res_df.printSchema()
    res_df.show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
