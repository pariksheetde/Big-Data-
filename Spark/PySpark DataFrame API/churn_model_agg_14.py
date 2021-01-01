import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.types import DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window


if __name__ == "__main__":
    print("Churn Modelling")

# set the SparkSession
churn_df = SparkSession.builder.appName("Churn Modeling").master("local[3]").getOrCreate()

# read the datafile from the location
churn = churn_df.read.csv(r"D:\Code\DataSet\SparkDataSet\ChurnModeling.csv", inferSchema=True, header=True)
churn.show(5, truncate=False)

# select the required columns
churn = churn.selectExpr("CustomerId", "Balance", "Surname", "CreditScore", "Geography", "Gender", "Age", "Tenure") \
    .filter((churn.Geography.isin("France", "Spain")) & (churn.Gender.isin("Male","Female"))) \
    .filter((churn.Age >= 18) & (churn.HasCrCard != 1) & (churn.IsActiveMember != 0) & (churn.Tenure > 5)) \
    .sort("Age")


churn_agg =  churn.select("Geography", "Gender", "Age", "Tenure") \
        .orderBy(f.col("Age").desc(),f.col("Tenure").asc()) \
        .groupBy("Geography", "Gender") \
        .agg(avg("Age").alias("Avg_Age"))

# , avg("Age").over(w).alias("Cnt")
churn_win = churn_agg.select("Geography", "Gender", "Avg_Age",dense_rank().over(Window.orderBy(desc("Avg_Age"))).alias("Dense_Rank"))

churn_win.show(100, truncate=False)
print(f'Records effected: {churn_win.count()}')

# .select(dense_rank().over(Window.orderBy(desc("Age"))).alias("Dense_Rank")) \
# w = Window.partitionBy("Geography", "Gender").orderBy("Age") # Just for clarity
