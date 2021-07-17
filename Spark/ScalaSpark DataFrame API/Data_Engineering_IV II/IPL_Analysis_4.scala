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

object IPL_Analysis_4 extends App {
  println("IPL Analysis 4")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "IPL Analysis 4")
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
  ipl_df.show(false)

  println(s"CALCULATE THE COUNT AND WINNING PERCENTAGE OF MATCHES WHEN HOME TEAM WINS")
  val home_team = ipl_df.selectExpr("team1 as Home_Team")
    .groupBy("Home_Team")
    .agg(
      count("Home_Team").alias("Matches_Played")
    ).sort(desc("Matches_Played"))
    home_team.show(false)

  val winner = ipl_df.selectExpr("winner as Team", "team1 as Host_Team")
    .groupBy("Team", "Host_Team")
    .agg(
      count("Team").alias("Matches_Won")
    )
    .where("Team = Host_Team")
    .drop("Team")
    .sort(desc("Matches_Won"))
    winner.show(false)

  val join_expr = home_team.col("Home_Team") === winner.col("Host_Team")
  val home_team_win_% = home_team.join(winner, join_expr, "inner")
    .withColumn("Win%", (col("Matches_Won") / col("Matches_Played")) * 100)
    .drop("Host_Team")
    .sort($"Win%".desc)
  home_team_win_%.show(false)

  spark.stop()

}
