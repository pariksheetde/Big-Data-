package Data_Engineering_2

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, count, max, mean, min, round, sum}

object FIFA_Stats_Aggregation_2 {
  println("FIFA World Cup Analysis")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "FIFA World Cup Analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

  //  read the FIFA datafile from the location
  val fifa_df = spark.read
    .format("csv")
    .options(Map(
      "header" -> "true",
      "mode" -> "PERMISSIVE",
      "path" -> "D:/DataSet/DataSet/SparkDataSet/FIFA_Stats.csv",
      "inferSchema" -> "true",
      "nullValue"-> "NA",
      "sep" -> ",",
      "compression" -> "snappy", // bzip2, gzip, lz4, deflate, uncompressed
      "dateFormat" -> "dd/MM/yyyy",
      "allowSingleQuotes" -> "true"
    ))
    .load()

  import spark.implicits._
  val sel_fifa = fifa_df.selectExpr("Year", "Datetime", "Stage as RoundRobin", "City", "Home_Team_Name", "Home_Team_Goals",
    "Away_Team_Goals", "Away_Team_Name",
    "Half_Time_Home_Goals as 1st_Half_Home_Goals", "Half_Time_Away_Goals as 1st_Half_Away_Goals",
    "Home_Team_Goals - Half_Time_Home_Goals as 2nd_Half_Home_Goals",
    "Away_Team_Goals - Half_Time_Away_Goals as 2nd_Half_Away_Goals")
    .withColumn("Goals", col("Home_Team_Goals") + col( "Away_Team_Goals"))

  sel_fifa.printSchema()
  sel_fifa.show()
  println(s"Records Effected: ${sel_fifa.count()}")

  //  Aggregating the goals scored in each year and home_team_name
  val agg_goals_year = sel_fifa.groupBy("Year", "Home_Team_Name")
    .agg(sum("Goals").as("Sum_Goals"),
      round(mean("Goals"),4).as("Avg_Goals"),
      count("Goals").alias("Cnt_Goals"),
      min("Goals").as("Min_Goals"))
    .sort(col("Year").asc,col("Sum_Goals").desc)

  agg_goals_year.show(false)
  println(s"Records Effected: ${agg_goals_year.count()}") // 366

  //  Aggregating the goals scored by home_team_name
  val agg_goals_home_team = sel_fifa.groupBy("Home_Team_Name")
    .agg(sum("Goals").as("Sum_Goals"),
      round(mean("Goals"),4).as("Avg_Goals"),
      count("Goals").alias("Cnt_Goals"),
      min("Goals").as("Min_Goals"))
    .sort(col("Sum_Goals").desc)

  agg_goals_home_team.show(false)
  println(s"Records Effected: ${agg_goals_home_team.count()}")

  def main(args: Array[String]): Unit = {
    //  max goals scored by home_team_name
    val sum_goals_home_team = sel_fifa.groupBy("Year", "Home_Team_Name")
      .agg(sum("Goals").as("Sum_Goals"))
      .sort(col("Year").asc,col("Sum_Goals").desc)

    sum_goals_home_team.show(false)
    println(s"Records Effected: ${sum_goals_home_team.count()}")

    spark.stop()
  }
}