package Bank

//import Bank.ChurnModeling.spark
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object churn_model_agg_10 extends App {
  println("Aggregation on Churn Modeling")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "churn_model_agg_10")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  //  read the datafile from the location
  //  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val churn = spark.read.option("inferSchema", "true").option("header", "true").csv("D:/Code/DataSet/SparkDataSet/ChurnModeling.csv")
  churn.show(10, truncate = false)

  val churn_sel = churn.selectExpr("CustomerId as Cust_ID", "Geography as Country", "Gender as Sex",
  "Age", "Tenure","Balance", "HasCrCard as Credit_Card",
    "IsActiveMember as Active", "EstimatedSalary as Salary")
    .filter("Country in ('France','Spain')")
    .filter("Age >= 18")  //& "Credit_Card = 1"
    .filter("Active = 1")
    .where("Balance > 0")
    .sort(col("Cust_ID").asc)
//    .show(10)
  println("Rocords returned: " + churn_sel.count())
  spark.stop()
}