package Bank

import org.apache.spark.sql.{SQLContext, SaveMode, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.spark_partition_id
import org.apache.spark.sql.functions.count

object Flight_Data_Analysis_4 {
  println("Flight Data Analysis for all source")
  println("Flight Data Analysis 4")

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

  def compute() = {
    val SQL_tab = flight_part.createOrReplaceTempView("flight_analysis")

    //  query from temp table
    val SQL_qry = spark.sql("select * from flight_analysis")

    //  count number of records in each partition
    SQL_qry.groupBy(spark_partition_id()).count().show()

    SQL_qry.write
      .format("csv")
      .partitionBy("Source", "destination")
      .mode(SaveMode.Overwrite)
      .option("path", "D:/DataSet/OutputDataset/flight_data/")
      .save()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
