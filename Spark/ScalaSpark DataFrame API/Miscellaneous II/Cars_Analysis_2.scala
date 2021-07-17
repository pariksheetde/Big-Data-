package Miscellaneous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr}

object Cars_Analysis_2 {

  println("Spark Structured DF 2")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Spark Structured 1")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  val cars_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .json("src/main/resources/cars")

  println("Number of partition:" + cars_df.rdd.getNumPartitions)

  def main(args: Array[String]): Unit = {
    cars_df.printSchema()
    cars_df.show(1, false)

    import spark.implicits._
    val origin_df = cars_df.select(col("Name"), col("Origin"),
      $"Year", $"Weight_in_lbs", expr("Weight_in_lbs * 2.2").as("Weight_in_Kgs"))
      .filter(col("Origin") =!= "USA")

    origin_df.printSchema()
    origin_df.show(10, false)
    println("Number of partition:" + origin_df.rdd.getNumPartitions)

    spark.stop()
  }
}
