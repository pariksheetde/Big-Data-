package DataSet_API

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._

object IPL_Data_Analysis_3 {
  println("IPL Analysis 3")

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
    .csv("D:/DataSet/DataSet/SparkDataSet/IndianPremierLeague.csv")

  //  limit the DS where city = Kolkata
  import spark.implicits._
  val filtered_ipl : Dataset[ipl_cc] = ipl_df.selectExpr("ID","City", "Schedule", "Player_of_Match", "Venue", "Neutral_Venue", "Team1",
    "Team2", "Toss_Winner", "Toss_Decision", "Winner", "Result", "Result_Margin", "Eliminator", "Method", "Umpire1", "Umpire2")
    .where("city != 'Kolkata' and Winner like 'Kolkata%'")
    .as[ipl_cc]

  def compute() = {
    //  select only the required column
    val sel_ipl = filtered_ipl.selectExpr("City", "Schedule", "Player_of_Match as MOM", "Venue", "Team1", "Team2", "Toss_Winner", "Winner")

    //  check the number of partitions
    println("Number of partitions: " + sel_ipl.rdd.getNumPartitions) // 1

    //  Increase the number of partitions
    val part_sel = sel_ipl.repartition(2)
    println("Number of partitions: " + part_sel.rdd.getNumPartitions) // 2

    //  count the number of rows in each partition
    part_sel.groupBy(spark_partition_id()).count().show()

    //  save the output DF to csv file
    part_sel.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      .option("header", "true")
      .option("path", "D:/DataSet/OutputDataset/IPL")
      .save()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}