package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofmonth, month, to_date, to_timestamp, year}
import org.apache.spark.sql.types.DateType

object Date_Manipulation_3 {
  println("Changing Data Format")
  println("Date Manipulation 3")

  val spark:SparkSession = SparkSession.builder()
    .master("local")
    .appName("Date Manipulation 3")
    .getOrCreate()

  def compute() = {
    import spark.sqlContext.implicits._

    val df = Seq(("2019-07-01"),
      ("2019-06-24"),
      ("2019-11-16"),
      ("2019-11-16")).toDF("date_of_joining")

    //Timestamp String to DateType
    df.withColumn("DOJ",
      to_date(col("date_of_joining"),"yyyy-MM-dd"))
      .show(false)

    val year_df = df.select(
      dayofmonth(col("date_of_joining")).alias("date"),
      month(col("date_of_joining")).alias("month"),
      year(col("date_of_joining")).alias("year")
    ).show(10, false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
