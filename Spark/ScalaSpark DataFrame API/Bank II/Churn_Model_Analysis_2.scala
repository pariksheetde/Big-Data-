package Bank

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext


object Churn_Model_Analysis_2  {
  println("Churn Modeling Basic")

  val sparkAppConfig = new SparkConf()

  sparkAppConfig.set("spark.app.name", "churn_model_agg_10")
  sparkAppConfig.set("spark.master", "local[3]")

  val spark = SparkSession.builder.config(sparkAppConfig).getOrCreate()

  case class Model(RowNumber: Int,	CustomerId: Int,	Surname: String,
                   CreditScore: Int,	Geography: String,	Gender: String,	Age: Int,
                   Tenure: Int,	Balance: Double,	NumOfProducts: Int,	HasCrCard: Int,
                   IsActiveMember: Int,	EstimatedSalary: Double,	Exited: Int
  )

//  read the datafile from the location
import spark.implicits._
val churn = spark.read.option("inferSchema", "true")
  .option("header", "true")
  .csv("D:/Code/DataSet/SparkDataSet/ChurnModeling.csv")
  .repartition(10)
  .as[Model]

  def main(args: Array[String]): Unit = {
    println(s"Number of Partitions ${churn.rdd.getNumPartitions}")
    val churn_show = churn.show(10, truncate = false)

    spark.stop()
  }

}
