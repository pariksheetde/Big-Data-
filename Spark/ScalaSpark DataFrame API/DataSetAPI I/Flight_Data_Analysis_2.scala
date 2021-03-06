package DataSet_API

import org.apache.spark.sql.{Dataset, Row, SQLContext, SaveMode, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.functions.count

object Flight_Data_Analysis_2 {
  println("Flight Data Analysis in DataSet API")
  println("Flight Data Analysis 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .enableHiveSupport()
    .getOrCreate()

  //spark.sql.shuffle.partitions configures the number of partitions that are used when shuffling data for joins or aggregations.
  spark.conf.set("spark.sql.shuffle.partitions",100)
  spark.conf.set("spark.default.parallelism",100)

  case class FlightRecs(Airline:String, Date_of_Journey:String, Source:String, Destination: String, Route:String,
                        Dep_Time:String, Arrival_Time: String, Duration:String, Total_Stops: String, Additional_Info: String)

  //  read the datafile from the location
  import spark.implicits._
  val flight_df : Dataset[Row] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("D:/DataSet/DataSet/SparkDataSet/flight.csv")

  def compute() = {
    val filtered_flight : Dataset[FlightRecs]= flight_df.select("Airline", "Date_of_Journey", "Source", "Destination", "Route",
      "Dep_Time", "Arrival_Time", "Duration", "Total_Stops", "Additional_Info").as[FlightRecs]

    // select the columns from dataset
    val fil_flight = filtered_flight.filter(row => row.Source == "Kolkata")
      .where("Destination != 'Delhi'")
      .select($"Date_of_Journey" ,$"Airline", $"Source", $"Destination")
    //    .show()

    fil_flight.repartition(10)

    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")
    spark.catalog.setCurrentDatabase("AIRLINE_DB")

    //  save the dataframe output to spark managed table
    fil_flight.write
      .format("csv")
      .mode(SaveMode.Overwrite)
      //    .partitionBy("Airline")
      .bucketBy(4, "Airline")
      .sortBy("Airline")
      .saveAsTable("flight_tbl")

    spark.catalog.listTables("AIRLINE_DB").show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
