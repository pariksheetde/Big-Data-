package Miscellaneous

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, substring}

object Cars_Analysis_1 {
  println("Spark Structured DF 1")

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

  def main(args: Array[String]): Unit = {
    cars_df.printSchema()
    cars_df.show(1, false)
    val origin_df = cars_df.select(col("Name"), col("Origin"),
      col("Weight_in_lbs"), col("Year"))
      .withColumn("Manufacture_Year", substring(col("Year"), 1, 4))
      .withColumn("Weight_in_kgs", col("Weight_in_lbs") * 2.2)

    origin_df.printSchema()
    origin_df.show(10, false)

    spark.stop()
  }
}
