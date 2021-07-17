package BigData

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.IntegerType

object Employee_Schema_1 extends App {
  println("Employee Details using Seq, Row & StructType")

  val spark = SparkSession.builder()
    .master("local[3]")
    .appName("USA Cars Details using StructType")
    .getOrCreate()

//  define data using seq and row
  val data = Seq(
      Row(100, "Monica", "Bellucci","London"),
      Row(110, "Kate", "Beckinsale","Paris"),
      Row(120, "Tom", "Hardy", "")
  )

  val schema = StructType(Array(
    StructField("ID",IntegerType,true),
    StructField("F_Name",StringType,true),
    StructField("L_Name",StringType,true),
    StructField("City", StringType, true)
  ))


  val df = spark.createDataFrame(spark.sparkContext.parallelize(data),schema)
  df.printSchema()
  df.show()
  spark.stop()
}
