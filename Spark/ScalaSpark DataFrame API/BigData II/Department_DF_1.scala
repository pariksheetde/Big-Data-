package BigData

import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.types.StringType
import java.util.Properties

object Department_DF_1 extends App {
  println("Reading from PostgreSQL DB")

  //  define spark session
  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("Stock Price Analysis")
    .getOrCreate()

  val connectionProperties = new Properties()
  connectionProperties.setProperty("Driver", "org.postgresql.Driver")

  val url = "jdbc.postgresql://localhost:5432/hr"

  val dept = spark.read
    .format("jdbc")
    .option("Driver", "org.postgresql.Driver")
    .option("url", "jdbc:postgresql://localhost:5432/hr")
    .option("user", "postgres")
    .option("dbtable", "public.departments")
    .load()
//  dept.show()
  spark.stop()
}
