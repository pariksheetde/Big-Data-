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

object IPL_Data_Analysis_5 {
  println("IPL Analysis 5")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "IPL Analysis 5")
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

    println(s"CALCULATE THE COUNT AND WINNING PERCENTAGE OF MATCHES WHEN AWAY TEAM WINS")
    val away_team = ipl_df.selectExpr("team2 as Away_Team")
      .groupBy("Away_Team")
      .agg(
        count("Away_Team").alias("Matches_Played")
      )
      .sort(desc("Matches_Played"))
    away_team.show(false)

    val away_team_win = ipl_df.selectExpr("team2 as Away_Team", "winner as Winning_Team")
      .groupBy("Away_Team", "Winning_Team")
      .agg(
        count("Away_Team").alias("Matches_Won")
      )
      .filter("Away_Team = Winning_Team")
      .drop("Away_Team")
      .sort(desc("Matches_Won"))
    away_team_win.show(false)

    val join_expr = away_team.col("Away_Team") === away_team_win.col("Winning_Team")
    val away_team_win_pct = away_team.join(away_team_win, join_expr, "inner")
    away_team_win_pct.selectExpr("Away_Team", "Matches_Played", "Matches_Won")
      .withColumn("Win%", (col("Matches_Won") / col("Matches_Played")) * 100)
      .show(false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}