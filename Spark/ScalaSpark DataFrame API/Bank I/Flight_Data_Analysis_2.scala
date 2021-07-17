package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object Flight_Data_Analysis_2 {
  println("Flight Data Analysis 2")

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

  //  repartition the dataframe
  val flight_part = flight_df.repartition(10)

  //  create a temp table
  val SQL_tab = flight_part.createOrReplaceTempView("flight_analysis")

  def compute() = {
    val SQL_qry = spark.sql("select * from flight_analysis where Source = 'Kolkata' and " +
      "Destination = 'Delhi'").show(false)
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
