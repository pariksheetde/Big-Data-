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

object IPL_Analysis_6 {
  println("IPL Analysis 6")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "IPL Analysis 6")
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

  def compute() = {
    import spark.implicits._
    val ipl_df = read_IPL("IndianPremierLeague.csv")
    ipl_df.show(false)

    println(s"CALCULATE MATCHES PLAYED FOR HOME AND AWAY TEAM")
    println(s"Home Team winning records")
    val home_team = ipl_df.selectExpr("team1 as Home_Team")
      .groupBy("Home_Team")
      .agg(
        count("Home_Team").as("Home_Matches_Played")
      ).sort(desc("Home_Matches_Played"))
    home_team.show(false)

    println(s"Away Team winning records")
    val away_team = ipl_df.selectExpr("team2 as Away_Team")
      .groupBy("Away_Team")
      .agg(
        count("Away_Team").as("Away_Matches_Played")
      ).sort(desc("Away_Matches_Played"))
    away_team.show(false)

    val join_expr = home_team.col("Home_Team") === away_team.col("Away_Team")
    val winning_pct = home_team.join(away_team, join_expr, "inner")
      .selectExpr("Home_Team as Team", "Home_Matches_Played", "Away_Matches_Played")
      .withColumn("Total_Matches_Played", (col("Home_Matches_Played") + col("Away_Matches_Played")))
      .drop("Home_Team")
      .sort($"Total_Matches_Played".desc)
      .show(false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
