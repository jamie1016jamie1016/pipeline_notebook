############ test.py ############
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("Halo Medals and SCD Test").getOrCreate()

def test_player_avg_kills(spark):
    input_data = [("player1", 10), ("player1", 20), ("player2", 30)]
    input_schema = ["player_gamertag", "player_total_kills"]
    input_df = spark.createDataFrame(input_data, input_schema)

    # Expected output
    expected_data = [("player2", 30.0), ("player1", 15.0)]
    expected_schema = ["player_gamertag", "avg_kills"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Perform aggregation
    result_df = input_df.groupBy("player_gamertag").agg(avg("player_total_kills").alias("avg_kills"))

    # Validate output
    assert result_df.collect() == expected_df.collect()

def test_most_played_playlist(spark):
    input_data = [("playlist1", "match1"), ("playlist1", "match2"), ("playlist2", "match3")]
    input_schema = ["playlist_id", "match_id"]
    input_df = spark.createDataFrame(input_data, input_schema)

    # Expected output
    expected_data = [("playlist1", 2), ("playlist2", 1)]
    expected_schema = ["playlist_id", "total_matches"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Perform aggregation
    result_df = input_df.groupBy("playlist_id").agg(count("match_id").alias("total_matches"))

    # Validate output
    assert result_df.collect() == expected_df.collect()

def test_killing_spree_map(spark):
    input_data = [("map1", "Killing Spree", 5), ("map1", "Killing Spree", 3), ("map2", "Other Medal", 2)]
    input_schema = ["mapid", "name", "medal_id"]
    input_df = spark.createDataFrame(input_data, input_schema)

    # Expected output
    expected_data = [("map1", 2)]
    expected_schema = ["mapid", "total_killing_sprees"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Perform filter and aggregation
    result_df = input_df.filter(col("name") == "Killing Spree").groupBy("mapid").agg(count("medal_id").alias("total_killing_sprees"))

    # Validate output
    assert result_df.collect() == expected_df.collect()

def test_scd_update(spark):
    actors_data = [
        ("1", 1970, "Film A", "good"),
        ("2", 1970, "Film B", "star")
    ]
    actor_films_data = [
        ("1", 1971, "Film C", 8.5),
        ("3", 1971, "Film D", 6.5)
    ]

    actors_schema = ["actorid", "year", "films", "quality_class"]
    actor_films_schema = ["actorid", "year", "films", "avg_rating"]

    actors_df = spark.createDataFrame(actors_data, actors_schema)
    actor_films_df = spark.createDataFrame(actor_films_data, actor_films_schema)

    actors_df.createOrReplaceTempView("actors")
    actor_films_df.createOrReplaceTempView("actor_films")

    # Expected Output
    expected_data = [
        ("1", 1971, "Film C", "star"),
        ("2", 1971, "Film B", "star"),
        ("3", 1971, "Film D", "average")
    ]
    expected_schema = ["actorid", "year", "films", "quality_class"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    # Execute SCD query
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

    # Validate Output
    assert result_df.collect() == expected_df.collect()
