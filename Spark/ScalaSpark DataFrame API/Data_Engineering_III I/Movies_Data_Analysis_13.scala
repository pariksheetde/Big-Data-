package Data_Engineering_3

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Window

object Movies_Data_Analysis_13 {
  println("Movies Data Analysis 13")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 13")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
    //    .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  //  read the movies.json datafile from the location
  val movies_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    //  .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .json("D:/DataSet/DataSet/SparkDataSet/movies.json")

  movies_df.printSchema()
  movies_df.show(false)

  //  calculate profit, rating earned for each director for each Genre
  val profit_null_df = movies_df.na.fill(Map(
    "Director" -> "Not Listed",
    "Major_Genre" -> "Not Listed",
    "US_DVD_Sales" -> 0,
    "US_Gross" -> 0,
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating" -> 0
  ))

  def compute() = {
    val release_dt_cln = profit_null_df.select(col("Title"), col("Director"), col("Release_date"),
      col("Major_Genre"), col("US_DVD_Sales"), col("US_Gross"))
      .withColumn("Date", when(substring(col("Release_Date"),1,2) contains("-"), concat(lit("0"),substring(col("Release_Date"),0,1)))
        otherwise(substring(col("Release_Date"), 1, 2)))
      .withColumn("Month", substring(col("Release_Date"),-6,3))
      .withColumn("Year",
        when(substring(col("Release_Date"), -2, 2) < 20, substring(col("Release_Date"), -2, 2) + 2000)
          otherwise(substring(col("Release_Date"),-2, 2) + 1900)
      ).drop("Release_Date")

    val movies_release_dt_df = release_dt_cln.select(col("Title"), col("Director"), col("Major_Genre"),
      col("Date"), col("Month"), col("Year"),
      (col("US_DVD_Sales") + col("US_Gross")).alias("Collection"))
      .withColumn("Date", expr("Date").cast(IntegerType))
      .withColumn("Year", expr("Year").cast(IntegerType))

    movies_release_dt_df.show(false)

    //  calculate collection per year, director
    println(s"calculate collection per year, director")
    val collection_year_dir = movies_release_dt_df.select(col("Year"), col("Director"), col("Collection"))
      .groupBy(col("Year"), col("Director"))
      .agg(
        sum(col("Collection")).alias("Collection")
      ).sort(col("Director").desc_nulls_last)
      .show(false)

    //  calculate collection per year, director, genre
    println(s"calculate collection per year, director, genre")
    val collection_year_dir_genre = movies_release_dt_df.select(col("Year"), col("Director"),
      col("Collection"), col("Major_Genre"))
      .groupBy(col("Year"), col("Director"), col("Major_Genre"))
      .agg(
        sum(col("Collection")).alias("Collection")
      ).sort(col("Director").desc_nulls_last)
      .where("Director = 'Steven Spielberg'")
      .show(100,false)

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}