import sys
from pyspark.sql import *
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions


if __name__ == "__main__":
    print("Churn Modelling")

# set the SparkSession
churn_df = SparkSession.builder.appName("Churn Modeling").master("local[3]").getOrCreate()

# read the datafile from the location
churn = churn_df.read.csv(r"D:\Code\DataSet\SparkDataSet\ChurnModeling.csv", inferSchema=True, header=True)
churn.show(5, truncate=False)

# select the required columns
churn_agg = churn.select("CustomerId", "Surname", "CreditScore", "Geography", "Gender", "Age", "Tenure") \
    .groupBy("Geography", "Gender") \
    .agg(avg("Age").alias("Avg_Age")) \
    .where("Avg_Age >= 40") \
    .orderBy("Avg_Age", ascending=False)

churn_agg.show(15)
print(f'Records effected: {churn_agg.count()}')
