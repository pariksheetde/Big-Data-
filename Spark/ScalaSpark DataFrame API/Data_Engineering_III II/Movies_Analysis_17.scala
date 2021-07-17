package Data_Engineering_III


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

object Movies_Analysis_17 {
  println("Movies Data Analysis 17")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Movies Data Analysis 17")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig)
//  .config("spark.sql.warehouse.dir","src/main/resources/warehouse")
    .getOrCreate()

  //  read the movies.json datafile from the location
  val movies_df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("dateFormat", "dd-MMM-yy")
    .option("mode", "PERMISSIVE") // dropMalFormed, permissive
    .option("sep", ",")
    .option("nullValue", "NA")
    .option("compression", "snappy") // bzip2, gzip, lz4, deflate, uncompressed
    .json("D:/Code/DataSet/SparkDataSet/movies.json")

  println(s"Number of Records: ${movies_df.count()}")
  movies_df.show(false)

  //  convert NULL to values for US_DVD_Sales and US_Gross
  val null_chk = movies_df.na.fill(Map(
    "US_Gross" -> 0,
    "US_DVD_Sales" -> 0
  ))

  def compute() = {
    val release_dt_cln = null_chk.select(col("Title"), col("Director"), col("Release_date"),
      col("Major_Genre"), col("US_DVD_Sales"), col("US_Gross"))
      .withColumn("Date", when(substring(col("Release_Date"), 1, 2) contains ("-"), concat(lit("0"), substring(col("Release_Date"), 0, 1)))
        otherwise (substring(col("Release_Date"), 1, 2)))
      .withColumn("Month", substring(col("Release_Date"), -6, 3))
      .withColumn("Year",
        when(substring(col("Release_Date"), -2, 2) < 20, substring(col("Release_Date"), -2, 2) + 2000)
          otherwise (substring(col("Release_Date"), -2, 2) + 1900)
      ).drop("Release_Date")

    val movies_release_dt_df = release_dt_cln.select(col("Title"), col("Director"), col("Major_Genre"),
      col("Date"), col("Month"), col("Year"),
      (col("US_DVD_Sales") + col("US_Gross")).alias("Collection"))
      .withColumn("Date", expr("Date").cast(IntegerType))
      .withColumn("Year", expr("Year").cast(IntegerType))
      .where("Director is not null")
    //  movies_release_dt_df.show(false)

    //  calculate collection partitioned by Director
    println(s"Calculate highest collection partitioned by Director")
    import spark.implicits._
    val coll_dir = movies_release_dt_df.select($"Director", $"Title", $"Collection")
      .withColumn("Rank", rank().over(Window.partitionBy($"Director").orderBy($"Collection".desc)))
      .show(false)

    //  calculate highest collection partitioned by Director
    println(s"Calculate highest collection partitioned by Director")
    import spark.implicits._
    val coll_highest_dir = movies_release_dt_df.select($"Director", $"Title", $"Collection")
      .withColumn("Rank", rank().over(Window.partitionBy($"Director").orderBy($"Collection".desc)))
      .filter($"Rank" === 1)
      .show(false)

    //  calculate highest collection partitioned by Director
    println(s"Calculate highest collection partitioned by Director in each Year")
    import spark.implicits._
    val coll_highest_dir_year = movies_release_dt_df.select($"Year", $"Director", $"Title", $"Collection")
      .withColumn("Rank", rank().over(Window.partitionBy($"Director", $"Year").orderBy($"Collection".desc, $"Year".asc)))
      .filter($"Rank" === 1 and $"Director" === "Steven Spielberg")
      .show(false)

    spark.stop()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
