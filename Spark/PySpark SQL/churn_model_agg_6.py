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
spark = SparkSession.builder.appName("Churn Modeling").master("local[3]").getOrCreate()

# read the datafile from the location
churn = spark.read.csv(r"D:\Code\DataSet\SparkDataSet\ChurnModeling.csv", header=True)
churn.show(5, truncate=False)

# select the required columns
churn_agg = churn.createOrReplaceTempView("Churn_Agg")

spark.sql("""select Geography,
                    case
                        when Gender == "Male" then "Male Associate"
                        when Gender == "Female" then "Female Associate"
                        else "Other"
                        end as Sex,
                    sum(Balance) as sum_balance
                    from
                    Churn_Agg
                    where Balance > 0
                    group by Geography, Gender
                    order by Geography, Gender
                    """)\
    .show(5)
