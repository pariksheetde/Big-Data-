package DataSetAPI

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object IPL_Analysis_6 extends App {
  println("IPL Analysis 6")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //spark.sql.shuffle.partitions configures the number of partitions that are used when shuffling data for joins or aggregations.
  spark.conf.set("spark.sql.shuffle.partitions",100)
  spark.conf.set("spark.default.parallelism",100)


  case class ipl_cc(ID:Integer, City: String, Schedule:String, Player_of_Match: String, Venue:String,
                    Neutral_Venue:Integer, Team1: String, Team2:String, Toss_Winner: String, Toss_Decision: String,
                    Winner: String, Result: String, Result_Margin: Integer, Eliminator: String, Method: String,
                    Umpire1: String, Umpire2: String)

  //  read the datafile from the location
  val ipl_df : Dataset[Row] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd/MM/yyyy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .csv("D:/Code/DataSet/SparkDataSet/IndianPremierLeague.csv")

//  limit the DS where city = Kolkata
  import spark.implicits._
//  shuffle partition
  spark.conf.set("spark.sql.shuffle.partitions", 3)
  val filtered_ipl : Dataset[ipl_cc] = ipl_df.selectExpr("ID","City", "Schedule", "Player_of_Match", "Venue", "Neutral_Venue", "Team1",
    "Team2", "Toss_Winner", "Toss_Decision", "Winner", "Result", "Result_Margin", "Eliminator", "Method", "Umpire1", "Umpire2")
    .where("city != 'Kolkata' and Winner like 'Kolkata%'")
    .as[ipl_cc]

//  display the schema and the show the records
  filtered_ipl.printSchema()
  filtered_ipl.show(false)
  spark.stop()
}
