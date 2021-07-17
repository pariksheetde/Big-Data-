package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object Flight_Data_Analysis_1 {
  println("Flight Data Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "flight data analysis")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //spark.sql.shuffle.partitions configures the number of partitions that are used when shuffling data for joins or aggregations.
  spark.conf.set("spark.sql.shuffle.partitions",100)
  spark.conf.set("spark.default.parallelism",100)


  //  read the datafile from the location
  val flight_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("D:/DataSet/DataSet/SparkDataSet/flight.csv")

  def compute() = {

// repartition the dataframe
  val flight_part = flight_df.repartition(10)

// where source is Kolkata
  val flight_kol = flight_part.where("Source == 'Kolkata'")
//  println("1st Number of Partitions " + flight_kol.rdd.getNumPartitions)

// select the required columns
  val flight_kol_sel = flight_kol.select("Airline", "Date_Of_Journey", "Source", "Destination", "Route", "Duration")
  val flight_kol_cnt = flight_kol_sel.select("Airline", "Date_Of_Journey", "Source", "Destination", "Route", "Duration")
      .groupBy("Destination")
      .count()
      .show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}

