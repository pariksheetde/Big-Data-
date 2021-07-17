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

object Locations_Data_Analysis_1 {
  println("Locations Data Analysis 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Locations Data Analysis 9")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    .getOrCreate()

//  define schema for locations.json
  val loc_jsob_schema = StructType(Array(
    StructField("loc_id", IntegerType),
    StructField("loc_name", StringType)
  ))

  case class Locations(loc_id: Int, loc_name: String)

  def compute() = {

    import spark.implicits._
    val loc_df = spark.read
      .schema(loc_jsob_schema)
      .json("D:/Code/DataSet/SparkStreamingDataSet/locations")
      .as[Locations]

    loc_df.printSchema()
    loc_df.show(10, false)
  }


  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
