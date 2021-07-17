package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions.count

object Churn_Model_1 {
  println("Churn Modeling 1")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "Churn Modeling 1")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  def compute() = {

    val churn = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("D:/DataSet/DataSet/SparkDataSet/ChurnModeling.csv")
    churn.show(10, truncate = false)

//  select the required columns
  val  churn_agg = churn.select("CustomerId", "Surname", "CreditScore",
      "Geography", "Gender", "Age", "Tenure")
      .filter("Geography == 'France'")
      .filter("CreditScore >= 501")
      .where("Gender == 'Male'" )
      .drop("Tenure")

    churn_agg.show(10, truncate = false)

    println(s"Records Effected: ${churn_agg.count()}")
  }

  def main(args: Array[String]): Unit = {
    compute()
  }
}
