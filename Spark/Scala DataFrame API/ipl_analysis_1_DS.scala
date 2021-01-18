package Bank

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SQLContext, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._

object ipl_analysis_1 extends App {
  println("Indian Premier League in DataSet API")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Indian Premier League")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //spark.sql.shuffle.partitions configures the number of partitions that are used when shuffling data for joins or aggregations.
  spark.conf.set("spark.sql.shuffle.partitions",100)
  spark.conf.set("spark.default.parallelism",100)

  case class ipl(City:String, Date:String, Winner:String, Venue:String, Team1:String, Team2: String,
                 Result: String, Neutral_Venue: String)

  //  read the datafile from the location
  import spark.implicits._
  val ipl_df  = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("mode", "FAILFAST") // PERMISSIVE, DROPMALFORMED, FAILFAST
    .csv("D:/Code/DataSet/SparkDataSet/IndianPremierLeague.csv").as[ipl]

  val ipl_sel_df = ipl_df.selectExpr("city","date", "winner", "venue as stadium", "team1", "team2",  "result", "neutral_venue")
    .show(20, truncate = false)
  
  spark.stop()
}
