package Data_Engineering_2

import org.apache.spark.sql.{DataFrame, Dataset, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{col, column, expr, monotonically_increasing_id, when, _}
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql._

object Data_Frame_2 {
  println("Data Frame 1 using StructType")

  //  create a spark session
  val sparkAppConf = new SparkConf()
  sparkAppConf.set("spark.app.name", "Data Frame 1 using StructType")
  sparkAppConf.set("spark.master", "local[3]")

  val spark = SparkSession.builder()
    .config(sparkAppConf)
    .getOrCreate()

  //  define the data
  val data = Seq(
    Row("Samsung", "Galaxy S8", "Android" ,65000, "15-10-2021"),
    Row("Apple", "IPhone 10 MAX", "iOS", 75000, "12-11-2020"),
    Row("Apple", "IPhone X", "iOS", 125000, "12-10-2017"),
    Row("Redmi", "Redmi 9", "Android", 10900,"12-12-2015"))

  //  define the schema
  val schema = StructType(List(
    StructField("Company", StringType),
    StructField("Model", StringType),
    StructField("OS", StringType),
    StructField("Price", IntegerType),
    StructField("Launch_DT", StringType)
  ))

  def compute() = {
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)

    val clean = df.select("Company", "Model", "OS", "Price", "Launch_DT")
      .withColumn("Date", substring(col("Launch_DT"), 1, 2))
      .withColumn("Month", substring(col("Launch_DT"), 4, 2))
      .withColumn("Year", substring(col("Launch_DT"), 7, 4))
      .drop("Launch_DT")

    val final_df = clean.selectExpr("*")
      .withColumn("Date", expr("Date").cast(IntegerType))
      .withColumn("Month", expr("Month").cast(IntegerType))
      .withColumn("Year", expr("Year").cast(IntegerType))

    final_df.printSchema()
    final_df.show()
    println(s"Number of rows in DF ${final_df.count()}")

  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
