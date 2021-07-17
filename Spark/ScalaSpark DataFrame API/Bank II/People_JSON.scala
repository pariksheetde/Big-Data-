package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.SQLContext.implicits._
import org.apache.spark.sql.functions.count

object People_JSON extends App {
  println("Reading from JSON file")
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("People's Data")
    .getOrCreate()
  val df = spark.read.json("D:/Code/DataSet/SparkDataSet/people.json")

  val df_show = df.show(df.count().toInt, false)
  print(s"Records Effected: ${df.count()}")

  spark.stop()
}
