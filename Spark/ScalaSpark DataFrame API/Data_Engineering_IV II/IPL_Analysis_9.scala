package Data_Engineering_IV

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

object IPL_Analysis_9 {
  println("IPL Analysis 9")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "IPL Analysis 9")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  def read_IPL(filename: String) = spark.read
    .format("csv")
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv(s"D:/Code/DataSet/SparkDataSet/$filename")

  import spark.implicits._
  val ipl_df = read_IPL("IndianPremierLeague.csv")
//  ipl_df.show(false)

  def compute() = {
    println(s"CALCULATE THE DATE MONTH YEAR")
    val schedule_cln_df = ipl_df.selectExpr("schedule as Schedule", "city as City", "venue as Venue", "team1 as Host_Team",
      "team2 as Visiting_Team", "winner as Winner", "result as Result")
      .withColumn("Date", substring(col("Schedule"), 1, 2))
      .withColumn("Month", substring(col("Schedule"), 4, 2))
      .withColumn("Year",
        when(substring(col("Schedule"), 7, 2) < 21, substring(col("Schedule"), 7, 2) + 2000)
          when(substring(col("Schedule"), 7, 2) < 100, substring(col("Schedule"), 7, 2) + 1900)
          otherwise (col("Schedule"))
      )
    //    schedule_cln_df.show(false)

    val schedule_df = schedule_cln_df.select("Date", "Month", "Year", "City", "Venue", "Host_Team", "Visiting_Team", "Winner", "Result")
      .withColumn("Date", expr("Date").cast(IntegerType))
      .withColumn("Month", expr("Month").cast(IntegerType))
      .withColumn("Year", expr("Year").cast(IntegerType))
    schedule_df.show(false)

    println(s"CALCULATE NUMBER OF MATCHES PLAYED IN EACH YEAR")
    val matches_cnt = schedule_df.select("Year")
      .groupBy("Year")
      .agg(
        count("Year").alias("Matches_Played_in_each_Year")
      ).sort($"Matches_Played_in_each_Year".desc)
    matches_cnt.show(false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
