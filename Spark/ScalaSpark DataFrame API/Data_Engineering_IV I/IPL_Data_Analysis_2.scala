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

object IPL_Data_Analysis_2 extends App {
  println("IPL Analysis 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "IPL Analysis 2")
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

  import spark.implicits._
  val ipl_df = read_IPL("IndianPremierLeague.csv")

  //  Matches played by each team
  import spark.implicits._
  println(s"Matches played by each host team")
  val host_played_each_team = ipl_df.select("team1")
    .groupBy("team1")
    .agg(
      count("team1").as("Matches_Played_by_each_host_team")
    )
    .sort(desc("Matches_Played_by_each_host_team"))
  //  host_played_each_team.show(false)

  //  Matches played by each team
  import spark.implicits._
  println(s"Matches played by each guest team")
  val guest_played_each_team = ipl_df.select("team2")
    .groupBy("team2")
    .agg(
      count("team2").as("Matches_Played_by_each_guest_team")
    )
    .sort(desc("Matches_Played_by_each_guest_team"))
  //  guest_played_each_team.show(false)

  //  Calculate number of matches played by each team
  import spark.implicits._
  println(s"Calculate number of matches played by each team")
  val matches_played_each_team = host_played_each_team.union(guest_played_each_team)
    .groupBy("team1")
    .agg(
      sum("Matches_Played_by_each_host_team").as("Matches_Played")
    )
    .orderBy(desc("Matches_Played"))

  val matches = matches_played_each_team.selectExpr("team1 as Team", "Matches_Played").show(false)

  //  which team has won most number of games
  import spark.implicits._
  println(s"Which team has won most number of games")
  val max_win_cnt = ipl_df.select("winner")
    .groupBy("winner")
    .agg(
      count("winner").alias("Win_Cnt")
    ).sort(desc("Win_Cnt"))
    .show(false)

  spark.stop()

}