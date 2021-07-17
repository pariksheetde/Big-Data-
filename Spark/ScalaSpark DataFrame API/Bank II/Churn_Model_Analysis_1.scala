package Bank

//import Bank.HelloWorld.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
//import org.apache.spark.sql.SQLContext.implicits._
import org.apache.spark.sql.functions.count

object Churn_Model_Analysis_1 {
  println("Churn Modeling 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Churn Modeling 1")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()
  //  read the datafile from the location
  //  val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  val churn = spark.read.option("inferSchema", "true").option("header", "true")
    .csv("D:/Code/DataSet/SparkDataSet/ChurnModeling.csv")
    .repartition(10)
  churn.show(10, truncate = false)
  println(s"Number of partitions ${churn.rdd.getNumPartitions}")

  //  select the required columns
  val  churn_agg = churn.select("CustomerId", "Surname", "CreditScore",
    "Geography", "Gender", "Age", "Tenure")
    .filter("Geography == 'France'")
    .filter("CreditScore >= 501")
    .where("Gender == 'Male'" )
    .drop("Tenure")

  def main(args: Array[String]): Unit = {
    churn_agg.show(20, truncate = false)
    println(s"Records Effected ${churn_agg.count()}")
    spark.stop()
  }
}
