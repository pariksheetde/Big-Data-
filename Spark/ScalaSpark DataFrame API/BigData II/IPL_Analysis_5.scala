package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}

object IPL_Analysis_5 {
  println("Aggregation: IPL Data Analysis")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Aggregation: IPL Data Analysis")
    .getOrCreate()

  //  define the schema for IPL DF
  val ipl_schema = StructType(List(
    StructField("id", IntegerType),
    StructField("city", StringType),
    StructField("schedule", StringType),
    StructField("player_of_match", StringType),
    StructField("venue", StringType),
    StructField("neutral_venue", StringType),
    StructField("team1", StringType),
    StructField("team2", StringType),
    StructField("toss_winner", StringType),
    StructField("toss_decision", StringType),
    StructField("winner", StringType),
    StructField("result", StringType),
    StructField("result_margin", StringType),
    StructField("eliminator", StringType),
    StructField("method", StringType),
    StructField("umpire1", StringType),
    StructField("umpire2", StringType)
  ))

  //  read the IPL DF
  spark.conf.set("spark.sql.legacy.timeParserPolicy","LEGACY")
  val ipl_df = spark.read
    .format("csv")
    .option("header", "true")
    .schema(ipl_schema)
    //.option("dateFormat", "yyyy-MM-dd")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .option("path","D:/Code/DataSet/SparkDataSet/IndianPremierLeague.csv")
    .load()

  def main(args: Array[String]): Unit = {
    val ipl_sel = ipl_df.selectExpr("city", "schedule", "player_of_match as MOM", "venue", "team1", "team2")
    val df = ipl_df.select("city").distinct().show()
  }


//  count number of man of the match except null
//  val cnt_city = ipl_sel.select(count("MOM").alias("MOM_cnt")).show() // 816

//  count number of man of the match except null
//  val cnt_city = ipl_df.selectExpr("count(player_of_match)").alias("cnt_MOM").show() // 816

//  unique count of city from the DF excluding null
//  val cnt_city = ipl_sel.select(countDistinct("city").alias("cnt")).show() // 33

//  count all the rows in the DF
//  val cnt = ipl_df.select(count("*")).show() // 816

//  select the distinct city name

//  select the distinct city name
//  val df = ipl_df.select(approx_count_distinct("city").alias("cnt_city")).show() \\34

}
