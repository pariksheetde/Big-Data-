package DataSetAPI

import org.apache.spark.sql.{Dataset, SparkSession}

object Guitar_Analysis extends App {
  println("Spark Structured DS 1")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Spark Structured 1")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()

  val guitars_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .json("src/main/resources/guitar")

  guitars_df.show(10, false)

  import spark.implicits._

  case class guitars_cc(guitarType: String, id: BigInt, make: String, model: String)

  val guitars_ds: Dataset[guitars_cc] = guitars_df.select("*").as[guitars_cc]
  guitars_ds.show(10, false)
  spark.stop()
}
