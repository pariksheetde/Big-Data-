package DataSetAPI

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object IPL_Data_Analysis_2 {
  println("Indian Premier League in DataSet API")
  println("IPL Analysis 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Indian Premier League")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //spark.sql.shuffle.partitions configures the number of partitions that are used when shuffling data for joins or aggregations.
  spark.conf.set("spark.sql.shuffle.partitions", 100)
  spark.conf.set("spark.default.parallelism", 100)

  case class ipl(City: String, Schedule: String, Winner: String, Venue: String, Team1: String, Team2: String,
                 Result: String, Neutral_Venue: String)

  //  read the datafile from the location

  import spark.implicits._

  val ipl_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-mm-yyyy")
    .option("mode", "FAILFAST")
    .csv("D:/Code/DataSet/SparkDataSet/IndianPremierLeague.csv").as[ipl]

  def compute() = {
    val ipl_sel_df = ipl_df.selectExpr("city", "schedule", "winner", "venue as stadium", "team1", "team2", "result", "neutral_venue")

    ipl_sel_df.show(20, truncate = false)
    ipl_sel_df.printSchema()

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}