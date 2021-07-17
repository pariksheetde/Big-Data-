package DataSetAPI

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Flight_Data_Analysis_7 {
  println("Flight Data Analysis in DataSet API")
  println("Flight Data Analysis DS 7")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //spark.sql.shuffle.partitions configures the number of partitions that are used when shuffling data for joins or aggregations.
  spark.conf.set("spark.sql.shuffle.partitions", 100)
  spark.conf.set("spark.default.parallelism", 100)

  case class FlightRecs(Airline: String, Date_of_Journey: String, Source: String, Destination: String, Route: String,
                        Dep_Time: String, Arrival_Time: String, Duration: String, Total_Stops: String, Additional_Info: String)

  //  read the datafile from the location

  import spark.implicits._

  val flight_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("D:/Code/DataSet/SparkDataSet/flight.csv").as[FlightRecs]

  val filtered_flight = flight_df.select("Airline", "Date_of_Journey", "Source", "Destination", "Route",
    "Dep_Time", "Arrival_Time", "Duration", "Total_Stops", "Additional_Info").as[FlightRecs]

  def main(args: Array[String]): Unit = {
    // select the columns from dataset
    val fil_flight = filtered_flight.filter(row => row.Source == "Kolkata")
      .where("Destination == 'Delhi'")
      .show()

    spark.stop()
  }

}
