package Data_Engineering_2

import java.sql.Date
import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.col
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._

object FIFA_Stats_2 {
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

  def main(args: Array[String]): Unit = {
    import spark.implicits._
    val sel_fifa = fifa_df.selectExpr("Year", "Datetime", "Stage as RoundRobin", "City", "Home_Team_Name", "Home_Team_Goals",
      "Away_Team_Goals", "Away_Team_Name",
      "Half_Time_Home_Goals as 1st_Half_Home_Goals", "Half_Time_Away_Goals as 1st_Half_Away_Goals",
      "Home_Team_Goals - Half_Time_Home_Goals as 2nd_Half_Home_Goals",
      "Away_Team_Goals - Half_Time_Away_Goals as 2nd_Half_Away_Goals")
      .withColumn("Goals", col("Home_Team_Goals") + col( "Away_Team_Goals"))

    //  filter the required data
    val all_fifa_cln_up = sel_fifa.selectExpr("Year", "Datetime", "RoundRobin", "City", "Home_Team_Name", "Away_Team_Name", "Home_Team_Goals",
      "Away_Team_Goals", "1st_Half_Home_Goals", "2nd_Half_Home_Goals", "1st_Half_Away_Goals", "2nd_Half_Away_Goals", "Goals")
      .where("Year <= 2000 and RoundRobin like 'Group%'")

    sel_fifa.printSchema()
    sel_fifa.show(false)
    println(s"Records effected: ${sel_fifa.count()}") // 430

    spark.stop()
  }
}