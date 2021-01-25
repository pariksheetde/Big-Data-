package Bank

import Bank.date_manupulation_1.{dateconversion, df}
import Bank.flight_data_analysis_2_DS.{FlightRecs, flight_df}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SQLContext, SparkSession}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.functions._

object netflix_analysis_1 extends App {
  println("Netfix Data Analysis Part 1")

  def dateconversion(df: DataFrame, fmt: String, fld: String): DataFrame = {
    df.withColumn(fld, to_date(col(fld),fmt))
  }

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Netfix Data Analysis Part 1")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  case class Movies(Show_ID: String,Type:String, Title:String, Director:String, Cast:String,
                    Country: String, Date_Added:String, Release_Year:String,
                    Rating: String, Duration:String, Listed_In: String, Description: String)

  //  read the datafile from the location
  import spark.implicits._
  val movies_df : Dataset[Row] = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("D:/Code/DataSet/SparkDataSet/netflix.csv")

  val all_movies : Dataset[Movies]= movies_df.select("Show_ID","Type", "Title", "Director", "Cast", "Country",
    "Date_Added", "Release_Year", "Rating", "Duration", "Listed_In", "Description").as[Movies]

  // filter out non null rows from Director column
  val filtered_movies = all_movies.select(column("Type"), col("Title"),
    column("Director"), column("Cast"), column("Country"),
    column("Date_Added"), column("Release_Year"), col("Rating"))
    .filter("Director is not null")

    filtered_movies.show()

  //  after conversion from string type to date type
  //    val func = dateconversion(filtered_movies, "ddMMyyyy", "Date_Added")
  //    func.printSchema()
  //    func.show(10)

  //  val modifiedDF = filtered_movies.withColumn("Date_Added", to_date(col("Date_Added"), "d/M/y"))
  //  val modifiedDF = filtered_movies.select(year(col("Date_Added")).alias("Date")).show()
  //  spark.stop()
}
