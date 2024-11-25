
# Dimensional Data Modeling Handbook

This handbook provides a comprehensive summary of the concepts, tasks, and methodologies involved in dimensional data modeling. It is intended as a reference guide to accompany the SQL code files for the assignment.

## Table of Contents

- [Introduction](#introduction)
- [Dataset Overview](#dataset-overview)
- [Key Concepts](#key-concepts)
- [Assignment Tasks Overview](#assignment-tasks-overview)
  - [Task 1: DDL for `actors` Table](#task-1-ddl-for-actors-table)
  - [Task 2: Cumulative Table Generation Query](#task-2-cumulative-table-generation-query)
  - [Task 3: DDL for `actors_history_scd` Table](#task-3-ddl-for-actors_history-scd-table)
  - [Task 4: Backfill Query for `actors_history_scd`](#task-4-backfill-query-for-actors_history-scd)
  - [Task 5: Incremental Query for `actors_history_scd`](#task-5-incremental-query-for-actors_history-scd)
- [Understanding Type 2 Slowly Changing Dimensions](#understanding-type-2-slowly-changing-dimensions)
- [Tips for Writing Complex SQL Queries](#tips-for-writing-complex-sql-queries)
- [Conclusion](#conclusion)

---

## Introduction

Dimensional data modeling is a key aspect of designing and maintaining data warehouses. This assignment involved creating tables and queries to model the `actor_films` dataset, track historical changes, and facilitate efficient analysis. Each task incrementally built upon the previous ones, resulting in a comprehensive understanding of the dataset and modeling techniques.

---

## Dataset Overview

The `actor_films` dataset contains details about actors and their films, including attributes like:

- **Actor Information**: Actor name and unique ID.
- **Film Information**: Film name, release year, votes, rating, and unique ID.
- **Yearly Data**: Data is available for each year, allowing cumulative tracking of changes.

---

## Key Concepts

### Dimensional Data Modeling

- **Star Schema**: A schema where fact tables are connected to dimension tables.
- **Slowly Changing Dimensions (SCD)**: A method to manage changes in dimension data over time, specifically Type 2 SCD in this assignment.

### SQL Techniques

- **Custom Data Types**: Using composite types and enums for complex structures.
- **Data Aggregation**: Using array and aggregate functions to collect related records.
- **Common Table Expressions (CTEs)**: Organizing queries into logical steps.
- **Window Functions**: Accessing previous and next rows for comparison.

---

## Assignment Tasks Overview

### Task 1: DDL for `actors` Table

**Objective**: Define a table to store cumulative information about actors, including their films, performance classification (`quality_class`), and activity status (`is_active`).

- **Custom Data Types**: Use composite types for storing films and enums for classifying performance.
- **Purpose**: Facilitate efficient storage and retrieval of actor data over time.

### Task 2: Cumulative Table Generation Query

**Objective**: Populate the `actors` table one year at a time, appending new films, updating performance classifications, and setting activity status.

- **Approach**: Use CTEs to compare current and previous data, merging results to maintain historical accuracy.
- **Key Technique**: Use `ARRAY_AGG` and conditional logic to update records.

### Task 3: DDL for `actors_history_scd` Table

**Objective**: Define a table for tracking historical changes in actor data using Type 2 SCD.

- **Type 2 SCD**: Each change in performance classification or activity status creates a new record.
- **Date Fields**: Include `start_date` and `end_date` to define the validity period for each record.

### Task 4: Backfill Query for `actors_history_scd`

**Objective**: Populate the `actors_history_scd` table with historical data in one operation.

- **Approach**: Use window functions (`LAG`, `LEAD`) to detect changes and group continuous periods with the same attributes.
- **Key Concept**: Assign `streak_id` to group unchanged periods and calculate their start and end dates.

### Task 5: Incremental Query for `actors_history_scd`

**Objective**: Update the `actors_history_scd` table with new data, handling changes incrementally.

- **Approach**: Compare current actor data with the latest records in the SCD table.
- **Record Handling**:
  - Close previous records if attributes change.
  - Insert new records for actors with changes or new entries.

---

## Understanding Type 2 Slowly Changing Dimensions

### What is Type 2 SCD?

Type 2 SCD tracks historical changes in dimension data by creating a new record for each change. It ensures:

- **Historical Accuracy**: Maintains a full history of changes.
- **Data Integrity**: Enables accurate historical reporting.

### Implementation in This Assignment

- **Attributes Tracked**: `quality_class` and `is_active` for each actor.
- **Record Lifecycle**:
  - **Active Records**: Have `end_date` set to `NULL`.
  - **Historical Records**: Have specific start and end dates.

---

## Tips for Writing Complex SQL Queries

1. **Planning and Design**:
   - Understand the requirements and break the task into smaller parts.
   - Sketch diagrams or pseudocode to visualize the data flow.

2. **Using CTEs**:
   - Organize complex queries into manageable steps.
   - Use sequential logic for clarity and debugging.

3. **Testing and Iteration**:
   - Validate each part of the query individually.
   - Test with sample data before scaling.

4. **Practice and Experience**:
   - Learn advanced SQL techniques like window functions and aggregate functions.
   - Study examples to improve understanding.

---

## Conclusion

This handbook provides a structured approach to understanding and implementing dimensional data modeling, particularly Type 2 SCD. By combining SQL techniques and best practices, the tasks demonstrate how to maintain historical data integrity and enable efficient analysis.

Use this guide as a reference to complement the SQL code files in the same folder. It serves as a foundation for future projects involving data modeling and warehousing.
