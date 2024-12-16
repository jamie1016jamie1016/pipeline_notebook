import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder.appName("Enhanced Halo Medals Test").getOrCreate()

def test_player_avg_kills(spark):
    input_data = [("player1", 10), ("player1", 20), ("player2", 30)]
    input_schema = ["player_gamertag", "player_total_kills"]
    input_df = spark.createDataFrame(input_data, input_schema)

    # Expected output
    expected_data = [("player2", 30.0), ("player1", 15.0)]
    expected_schema = ["player_gamertag", "avg_kills"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    result_df = input_df.groupBy("player_gamertag").agg(avg("player_total_kills").alias("avg_kills"))

    assert result_df.collect() == expected_df.collect()

def test_killing_spree_map(spark):
    medals_data = [("medal1", "Killing Spree"), ("medal2", "Killing Spree"), ("medal3", "Other")]
    maps_data = [("map1", "Map A"), ("map2", "Map B")]

    medals_schema = ["medal_id", "name"]
    maps_schema = ["mapid", "name"]

    medals_df = spark.createDataFrame(medals_data, medals_schema)
    maps_df = spark.createDataFrame(maps_data, maps_schema)

    result_df = medals_df.filter(col("name") == "Killing Spree").join(maps_df, medals_df.medal_id == maps_df.mapid, "inner") \
        .groupBy("name").agg(count("medal_id").alias("total_killing_sprees"))

    expected_data = [("Killing Spree", 2)]
    expected_schema = ["name", "total_killing_sprees"]
    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert result_df.collect() == expected_df.collect()

def test_partition_optimization(spark, tmpdir):
    input_data = [("playlist1", "match1"), ("playlist2", "match2"), ("playlist3", "match3")]
    input_schema = ["playlist_id", "match_id"]
    input_df = spark.createDataFrame(input_data, input_schema)

    partitioned_path = tmpdir.join("partitioned_output").strpath
    input_df.repartition(2, "playlist_id").sortWithinPartitions("playlist_id").write.partitionBy("playlist_id").csv(partitioned_path, header=True)

    # Read back the partitioned data and validate
    partitioned_df = spark.read.csv(partitioned_path, header=True, inferSchema=True)
    assert partitioned_df.rdd.getNumPartitions() == 2
