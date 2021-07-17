package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext

object Churn_Model_2 extends App {
  println("Churn Modeling 2")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Churn Modeling 2")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

// read the datafile from the location
  val churn = spark.read.option("inferSchema", "true")
    .option("header", "true")
    .csv("D:/DataSet/DataSet/SparkDataSet/ChurnModeling.csv")
  churn.show(10, truncate = false)
  spark.stop()
}
