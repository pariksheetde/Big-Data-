package Data_Engineering_4

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

object IPL_Data_Analysis_7 {
  println("IPL Analysis 7")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "IPL Analysis 7")
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
    .csv(s"D:/DataSet/DataSet/SparkDataSet/$filename")

  def compute() = {
    import spark.implicits._
    val ipl_df = read_IPL("IndianPremierLeague.csv")
    ipl_df.show(false)

    println(s"COMPUTE WINNING PERCENTAGE FOR HOME & AWAY TEAM")
    println(s"MATCHES PLAYED BY HOME TEAM")
    val home_team = ipl_df.selectExpr("team1 as Home_Team")
      .groupBy("Home_Team")
      .agg(
        count("Home_Team").alias("Home_Matches_Played")
      ).sort($"Home_Matches_Played".desc)
    home_team.show(false)

    println(s"MATCHES PLAYED BY AWAY TEAM")
    val away_team = ipl_df.selectExpr("team2 as Away_Team")
      .groupBy("Away_Team")
      .agg(
        count("Away_Team").alias("Away_Matches_Played")
      ).sort($"Away_Matches_Played".desc)
    away_team.show(false)

    println(s"MATCHES WON BY HOME TEAM")
    val home_win = ipl_df.selectExpr("team1 as Host_Team", "winner")
      .groupBy("Host_Team", "winner")
      .agg(
        count("winner").as("Host_Team_Won")
      ).filter("Host_Team = winner")
    home_win.show(false)

    println(s"MATCHES WON BY VISITING TEAM")
    val away_win = ipl_df.selectExpr("team2 as Guest_Team", "winner")
      .groupBy("Guest_Team", "winner")
      .agg(
        count("winner").as("Guest_Team_Won")
      ).filter("Guest_Team = winner")
    away_win.show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}