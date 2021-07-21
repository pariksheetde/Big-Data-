# Databricks notebook source
import sys
from pyspark.sql import *
import pyspark
from pyspark.sql.functions import spark_partition_id
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pyspark.sql.functions

# read from movies file
movies_df = spark.read.csv("/FileStore/tables/movies.csv", header = True)
movies_df.printSchema()
movies_df.show(truncate = False)

# read from ratings file
ratings_df = spark.read.csv("/FileStore/tables/ratings.csv", header = True)
ratings_df.printSchema()
ratings_df.show(truncate = False)



# COMMAND ----------

# join opeartion between movies DF and ratings DF
# rename movieID column in ratings_df DF
ratings_df = ratings_df.withColumnRenamed("movieId", "movie_Seq")
ratings_df.printSchema()
movies_rating_DF = movies_df.join(ratings_df, movies_df.movieId == ratings_df.movie_Seq, "inner") \
.selectExpr("movieId as MovieID", "userId as UserID", "title as Movie_Name", "genres as Genres", "rating as Rating", "timestamp as Created_TS" )
movies_rating_DF.show(truncate = False)
