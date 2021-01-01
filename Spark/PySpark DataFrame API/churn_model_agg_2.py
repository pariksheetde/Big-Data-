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
churn = churn_df.read.csv(r"D:\Code\DataSet\SparkDataSet\ChurnModeling.csv", header=True)
churn.show(5, truncate=False)

# select the required columns
churn_agg = churn.select("CustomerId", "Surname", "CreditScore", "Geography", "Gender", "Age", "Tenure") \
    .where(churn.Geography == "France") \
    .groupBy("Geography", "Gender") \
    .agg(avg("Age").alias("Avg_Age")) \
    .orderBy(asc("Avg_Age"))

churn_agg.show(15)
print(f'Records effected: {churn_agg.count()}')