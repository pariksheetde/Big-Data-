import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import *
from pyspark.sql.types import DateType
from pyspark.sql.functions import *
from pyspark.sql.window import Window


if __name__ == "__main__":
    print("Games Analysis")

# set the SparkSession
spark = SparkSession.builder.appName("Games Analysis").master("local[3]").getOrCreate()

games = spark.read.csv(r"D:\Code\DataSet\SparkDataSet\NHL\game.csv", inferSchema=True, header=True)
games.show(5, truncate=False)

# drop unwanted columns from the DF
games = games.drop("date_time_GMT", "away_team_id", "home_team_id", "venue_link", "venue_time_zone_id", "venue_time_zone_offset", "venue_time_zone_tz")

# select the required columns
games_sel = games.select("game_id", "season", "type", "away_goals", "home_goals", "outcome", "home_rink_side_start") \
                 .filter(games.type != "P") \
                 .where("home_rink_side_start in ('right', 'left')") \
                 .where("away_goals >= 9 and away_goals > home_goals")

# create temp table
SQL_tab = games_sel.createOrReplaceTempView("games")

# query from temp table
SQL_qry = spark.sql("""
                    select 
                    game_id, season, 
                    substr(season, 1, 4) as start_year,
                    substr(season, 5, 8) as end_year,
                    type, away_goals, home_goals, outcome, home_rink_side_start
                    from games
                    order by season asc
                    """)

SQL_qry.show(10, truncate=False)
print(f"Records Effected: {SQL_qry.count()}")
print(f"Number of partition: {SQL_qry.rdd.getNumPartitions()}")
