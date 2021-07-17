package DataSetAPI

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.functions.rank
import org.apache.spark.sql.expressions.Window

object Locations_Data_Analysis_2 {
  println("Locations Data Analysis 2")

  val spark = SparkSession
    .builder()
    .master("local[*]")
    .appName("Locations Data Analysis 2")
    .getOrCreate()

//  define schema for locations.json
  val loc_json_schma = StructType(Array(
    StructField("loc_id", IntegerType),
    StructField("loc_name", StringType)
  ))

  case class Locations(loc_id: Int, loc_name: String)

  def compute() = {
    import spark.implicits._
    val loc_df = spark.read
      .schema(loc_json_schma)
      .json("D:/Code/DataSet/SparkStreamingDataSet/locations")
      .as[Locations]

    val filter_loc_name = loc_df.filter(loc_df("loc_name") isin  ("London", "Berlin"))
    filter_loc_name.show(10, false)

  }

  def main(args: Array[String]): Unit =
    {
      compute()
      spark.stop()
  }
}
