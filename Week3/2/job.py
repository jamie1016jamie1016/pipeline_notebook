############ job.py ############
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, broadcast, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Halo Medals and SCD Analysis") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .getOrCreate()

# Load data
maps_df = spark.read.csv("/mnt/data/maps.csv", header=True, inferSchema=True)
match_details_df = spark.read.csv("/mnt/data/match_details.csv", header=True, inferSchema=True)
matches_df = spark.read.csv("/mnt/data/matches.csv", header=True, inferSchema=True)
medals_matches_players_df = spark.read.csv("/mnt/data/medals_matches_players.csv", header=True, inferSchema=True)
medals_df = spark.read.csv("/mnt/data/medals.csv", header=True, inferSchema=True)

# Explicitly broadcast join maps and medals
maps_broadcast = broadcast(maps_df)
medals_broadcast = broadcast(medals_df)
data_with_maps = match_details_df.join(maps_broadcast, match_details_df.match_id == maps_df.mapid, "left")
data_with_medals = medals_matches_players_df.join(medals_broadcast, medals_matches_players_df.medal_id == medals_df.medal_id, "left")

# Bucket joins
match_details_df.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_match_details")
matches_df.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_matches")
medals_matches_players_df.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_medals")

# Aggregations
# 4a. Which player averages the most kills per game?
player_avg_kills = match_details_df.groupBy("player_gamertag").agg(avg("player_total_kills").alias("avg_kills"))
player_avg_kills_result = player_avg_kills.orderBy(desc("avg_kills"))
player_avg_kills_result.show()

# 4b. Which playlist gets played the most?
most_played_playlist = matches_df.groupBy("playlist_id").agg(count("match_id").alias("total_matches"))
most_played_playlist_result = most_played_playlist.orderBy(desc("total_matches"))
most_played_playlist_result.show()

# 4c. Which map gets played the most?
most_played_map = data_with_maps.groupBy("name").agg(count("match_id").alias("total_matches"))
most_played_map_result = most_played_map.orderBy(desc("total_matches"))
most_played_map_result.show()

# 4d. Which map has the highest number of Killing Spree medals?
killing_spree_map = data_with_medals.filter(col("name") == "Killing Spree").groupBy("mapid").agg(count("medal_id").alias("total_killing_sprees"))
killing_spree_map_result = killing_spree_map.orderBy(desc("total_killing_sprees"))
killing_spree_map_result.show()

# SCD Logic
actors_df = spark.read.csv("/mnt/data/actors.csv", header=True, inferSchema=True)
actor_films_df = spark.read.csv("/mnt/data/actor_films.csv", header=True, inferSchema=True)

actors_df.createOrReplaceTempView("actors")
actor_films_df.createOrReplaceTempView("actor_films")

scd_query = """
    WITH current_data AS (
        SELECT actorid, year, films, avg_rating
        FROM actor_films
        WHERE year = 1971
    ),
    scd_update AS (
        SELECT
            a.actorid,
            COALESCE(c.year, a.year + 1) AS year,
            COALESCE(c.films, a.films) AS films,
            CASE
                WHEN c.avg_rating > 8 THEN 'star'
                WHEN c.avg_rating > 7 THEN 'good'
                WHEN c.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END AS quality_class
        FROM actors a
        LEFT JOIN current_data c ON a.actorid = c.actorid
    )
    SELECT * FROM scd_update
"""
result_df = spark.sql(scd_query)
result_df.write.csv("/mnt/data/actors_scd_output.csv", header=True)

spark.stop()