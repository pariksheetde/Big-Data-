package Data_Engineering_II

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructField, StructType}

object Data_Frame_1 extends App {
  println("Data Frame 1 using Seq Tuple")

  //  create a spark session
  val sparkAppConf = new SparkConf()
  sparkAppConf.set("spark.app.name", "Data Frame 1 using Seq Tuple")
  sparkAppConf.set("spark.master", "local[3]")

  val spark = SparkSession.builder()
    .config(sparkAppConf)
    .getOrCreate()

//  define the dataset
  val dataset = Seq(
    ("Monica", "Bellucci", 2500000),
    ("Pamela", "Parker", 2250000),
    ("Kate", "Beckinsale", 1700000),
    ("Audry", "Hepburn", 1750000),
    ("Jennifer", "Lawrence", 1957000),
    ("Carmen", "Electra", 1550000),
    ("Jennifer", "Garner", 3225000),
    ("Michelle", "Obama", 1400500),
    ("Kirsten", "Steward", 2150000),
    ("Sophia", "Turner", 2945000),
  )
// define the data frame
  val df = spark.createDataFrame(dataset).toDF("F_Name", "L_Name", "Salary")
  val sel_df = df.where("Salary > 2500000")
  sel_df.printSchema()
  sel_df.show()

  spark.stop()

}
