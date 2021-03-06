package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object People_JSON extends App {
  println("let's test json file")
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Churn Modeling")
    .getOrCreate()

  val people_df = spark.read.json("D:/DataSet/DataSet/SparkDataSet/people.json")

    people_df.show(10, false)
  spark.stop()
}
