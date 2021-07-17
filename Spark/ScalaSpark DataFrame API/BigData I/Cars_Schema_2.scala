package BigData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

object Cars_Schema_2 {
  println("German Cars Details using Seq & StructType")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("German Cars Details using StructType")
    .getOrCreate()

  // create the data
  val data = Seq(
    (100, "BMW", "BMW X4 Roadstar"),
    (110, "Audi", "Audi Q4"),
    (120, "Mercedes", "" )
  )

  // define the DF
  val emp_df = spark.createDataFrame(data)

  def compute() = {
    // implicit DF conversion
    import spark.implicits._
    val col_name = data.toDF("ID", "Name", "City")
    col_name.show()
  }

  def main(args: Array[String]): Unit = {
    compute()
    spark.stop()
  }
}
