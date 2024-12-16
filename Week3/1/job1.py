from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, broadcast, desc

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Enhanced Halo Medals Analysis") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \  # Task 1: Disable auto broadcast join
    .getOrCreate()

# Load datasets
maps_df = spark.read.csv("/mnt/data/maps.csv", header=True, inferSchema=True)
match_details_df = spark.read.csv("/mnt/data/match_details.csv", header=True, inferSchema=True)
matches_df = spark.read.csv("/mnt/data/matches.csv", header=True, inferSchema=True)
medals_matches_players_df = spark.read.csv("/mnt/data/medals_matches_players.csv", header=True, inferSchema=True)
medals_df = spark.read.csv("/mnt/data/medals.csv", header=True, inferSchema=True)

# Task 2: Explicitly broadcast join medals and maps tables
maps_broadcast = broadcast(maps_df)
medals_broadcast = broadcast(medals_df)

data_with_maps = match_details_df.join(maps_broadcast, match_details_df.map_id == maps_broadcast.mapid, "left")
data_with_medals = medals_matches_players_df.join(medals_broadcast, medals_matches_players_df.medal_id == medals_broadcast.medal_id, "left")

# Task 3: Bucket join on match_id
match_details_df.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_match_details")
matches_df.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_matches")
medals_matches_players_df.write.bucketBy(16, "match_id").sortBy("match_id").mode("overwrite").saveAsTable("bucketed_medals_matches_players")

bucketed_match_details = spark.table("bucketed_match_details")
bucketed_matches = spark.table("bucketed_matches")
bucketed_medals_matches_players = spark.table("bucketed_medals_matches_players")

bucketed_join = bucketed_match_details.join(bucketed_matches, "match_id", "inner").join(bucketed_medals_matches_players, "match_id", "inner")
bucketed_join.show()

# Task 4: Aggregations
# 4a. Which player averages the most kills per game
player_avg_kills = match_details_df.groupBy("player_gamertag").agg(avg("player_total_kills").alias("avg_kills")).orderBy(desc("avg_kills"))
player_avg_kills.show()

# 4b. Playlist with the most plays
most_played_playlist = matches_df.groupBy("playlist_id").agg(count("match_id").alias("total_matches")).orderBy(desc("total_matches"))
most_played_playlist.show()

# 4c. Which map gets played the most
most_played_map = data_with_maps.groupBy("name").agg(count("match_id").alias("total_matches")).orderBy(desc("total_matches"))
most_played_map.show()

# 4d. Which map has the highest number of Killing Spree medals
killing_spree_map = data_with_medals.filter(col("name") == "Killing Spree") \
    .join(maps_broadcast, data_with_medals.map_id == maps_broadcast.mapid, "inner") \
    .groupBy("mapid", "name").agg(count("medal_id").alias("total_killing_sprees")) \
    .orderBy(desc("total_killing_sprees"))
killing_spree_map.show()

# Task 5: Optimizations
partitioned_playlist = matches_df.repartition(4, "playlist_id").sortWithinPartitions("playlist_id")
partitioned_playlist.write.partitionBy("playlist_id").csv("/mnt/data/partitioned_playlist.csv", header=True)

partitioned_map = maps_df.repartition(2, "mapid").sortWithinPartitions("mapid")
partitioned_map.write.partitionBy("mapid").csv("/mnt/data/partitioned_map.csv", header=True)

spark.stop()
