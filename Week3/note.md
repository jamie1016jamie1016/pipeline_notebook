
# 🚀 Apache Spark Infrastructure Homework: Week 3

Welcome to Week 3 of the Apache Spark Infrastructure homework series! This week’s focus was on implementing and optimizing PySpark workflows for various data processing tasks. The assignment builds on concepts from the previous weeks, emphasizing **broadcast joins**, **bucket joins**, and **aggregation queries**. 

---

## 📚 Dataset Overview

The datasets used for this assignment include:

1. **match_details**: Contains player-specific performance data for each match.
2. **matches**: Details about each match played.
3. **medals_matches_players**: Data linking medals earned by players in matches.
4. **medals**: Metadata for each medal type.
5. **maps**: Metadata for game maps.

---

## 🏗️ Assignment Tasks Overview

### Task 1: Disable Auto Broadcast Join
- **Goal**: Disable automatic broadcast joins using Spark’s configuration setting.
- **Implementation**: Set `spark.sql.autoBroadcastJoinThreshold` to `-1`.

---

### Task 2: Explicit Broadcast Joins
- **Goal**: Manually broadcast the `medals` and `maps` tables to optimize joins.
- **Implementation**: Use Spark’s `broadcast()` function to explicitly broadcast these tables and join them efficiently.

---

### Task 3: Bucket Joins on `match_id`
- **Goal**: Bucket the `match_details`, `matches`, and `medals_matches_players` tables by `match_id` with 16 buckets.
- **Implementation**:
  - Use `bucketBy` and `sortBy` to create bucketed tables.
  - Perform bucket joins to leverage bucketing optimizations.

---

### Task 4: Aggregation Queries
Perform aggregations to answer the following questions:
1. **Query 4a**: Which player averages the most kills per game?
2. **Query 4b**: Which playlist is played the most?
3. **Query 4c**: Which map is played the most?
4. **Query 4d**: On which map do players earn the most "Killing Spree" medals?

---

### Task 5: Optimization and Partitioning
- **Goal**: Optimize data size using partitioning strategies and sorting within partitions.
- **Implementation**:
  - Use `repartition` and `sortWithinPartitions` for low-cardinality fields (e.g., `playlist_id`, `mapid`).
  - Write partitioned datasets to improve storage and query performance.

---

## 🛠️ Code Files and Structure

### 1. `job.py`
- Contains the main PySpark job, implementing:
  - **Configuration**: Disabling auto broadcast joins.
  - **Joins**: Explicit broadcast joins and bucket joins.
  - **Aggregations**: Queries 4a–4d.
  - **Optimization**: Partitioning and sorting strategies.

---

### 2. `test.py`
- Contains unit tests for key queries and workflows:
  - **Query 4a**: Tests for the highest average kills per game.
  - **Query 4b**: Tests for the most-played playlist.
  - **Query 4d**: Tests for filtering medals by "Killing Spree."
  - **Optimization**: Validates partitioning strategies.

---

## 🔍 Key Concepts and Techniques

1. **Broadcast Joins**:
   - Manually optimize joins for small tables like `maps` and `medals`.

2. **Bucket Joins**:
   - Improve performance by bucketing and sorting tables on `match_id`.

3. **Aggregations**:
   - Perform efficient groupings and calculations to derive insights.

4. **Partitioning and Sorting**:
   - Minimize shuffle costs and reduce storage sizes using partitioning strategies.

5. **Testing**:
   - Validate query correctness and optimization impacts with PySpark unit tests.

---

## 📈 Results and Insights

- **Aggregation Queries**:
  - Query results provided insights into player performance, playlist popularity, map usage, and medal distributions.
- **Optimization**:
  - Bucket joins and explicit broadcasting improved performance by reducing shuffle costs.
  - Partitioning strategies decreased data size and enhanced query efficiency.

---

## 💡 Next Steps

- Explore additional partitioning strategies for high-cardinality fields.
- Extend testing to include stress tests with larger datasets.
- Experiment with additional optimization techniques like caching and checkpointing.

---

Feel free to reach out if you have questions or need clarifications on the tasks or implementation. 🎉
