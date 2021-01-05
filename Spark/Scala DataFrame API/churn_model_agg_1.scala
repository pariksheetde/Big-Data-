package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object HelloWorld extends App {
  println("Churn Modeling")
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Churn Modeling")
    .getOrCreate()

//  read the datafile from the location
//val sqlContext = new org.apache.spark.sql.SQLContext(sc)
val churn = spark.read.option("inferSchema", "true").option("header", "true").csv("D:/Code/DataSet/SparkDataSet/ChurnModeling.csv")
  churn.show(10, truncate = false)
}
