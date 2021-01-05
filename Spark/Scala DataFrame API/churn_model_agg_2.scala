package Bank

//import Bank.HelloWorld.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.SQLContext.implicits._
//import sqlContext.implicits._

object ChurnModeling extends App {
  println("Hello Spark")
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Churn Modeling")
    .getOrCreate()

  //  read the datafile from the location
//  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val churn = spark.read.option("inferSchema", "true").option("header", "true").csv("D:/Code/DataSet/SparkDataSet/ChurnModeling.csv")
  churn.show(10, truncate = false)

//  select the required columns
  val  churn_agg = churn.select("CustomerId", "Surname", "CreditScore",
    "Geography", "Gender", "Age", "Tenure")
    .filter("Geography == 'France'")
    .filter("CreditScore >= 501")
    .where("Gender == 'Male'" )
    .drop("Tenure")
    .show(10, truncate = false)
//  println(churn_agg.count())
}
