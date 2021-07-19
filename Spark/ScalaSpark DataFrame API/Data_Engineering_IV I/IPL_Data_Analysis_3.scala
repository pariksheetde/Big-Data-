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

object IPL_Data_Analysis_3  {
  println("IPL Analysis 3")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "IPL Analysis 3")
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

  //  Calculate number of matches played by each team
  import spark.implicits._
  println(s"Calculate number of matches played by each team")
  val matches_played_each_team = host_played_each_team.union(guest_played_each_team)
    .groupBy("team1")
    .agg(
      sum("Matches_Played_by_each_host_team").as("Matches_Played")
    )
    .orderBy(desc("Matches_Played"))

  val matches = matches_played_each_team.selectExpr("team1 as Team", "Matches_Played")
  matches.show(false)

  def main(args: Array[String]): Unit = {
    //  which team has won most number of games
    import spark.implicits._
    println(s"Which team has won most number of games")
    val max_win_cnt = ipl_df.selectExpr("winner as Team")
      .groupBy("Team")
      .agg(
        count("Team").alias("Win_Cnt")
      ).sort(desc("Win_Cnt"))
    max_win_cnt.show(false)

    //  rename department_id in cln_emp_df to dept_id
    val cln_match = matches.withColumnRenamed("Team", "Club")
    val join_expr = cln_match.col("Club") === max_win_cnt.col("Team")

    // write the inner join condition between cln_dept_df & cln_emp_dept_id
    val match_results = cln_match.join(max_win_cnt, join_expr, "inner")
      .selectExpr("Team", "Matches_Played", "Win_Cnt")
      .orderBy(desc("Matches_Played"))
    match_results.show(false)

    val match_stat = match_results.selectExpr("Team", "Matches_Played as Matches", "Win_Cnt as Win")
      .withColumn("Win_%", (col("Win") / col("Matches"))*100)
      .sort($"Win_%".desc)
      .show(false)

    spark.stop()
  }
}