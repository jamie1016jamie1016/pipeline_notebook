-- Task 8: Incremental Query to Load host_activity_reduced Day by Day

-- Parameters
-- Set the date for which you want to process the data
-- Replace '2023-01-03' with the date you are processing
WITH params AS (
    SELECT
        '2023-01-06'::DATE AS processing_date
),
-- Step 1: Extract Daily Data from events
daily_stats AS (
    SELECT
        e.host,
        DATE_TRUNC('month', p.processing_date) AS month_start,
        EXTRACT(DAY FROM p.processing_date)::INT AS day_index,
        COUNT(1) AS hit_count,
        COUNT(DISTINCT e.user_id) AS unique_visitors
    FROM
        events e
        CROSS JOIN params p
    WHERE
        DATE(e.event_time) = p.processing_date
        AND e.host IS NOT NULL
    GROUP BY
        e.host, p.processing_date
),
-- Step 2: Prepare Data for Upsert
upsert_data AS (
    SELECT
        ds.month_start,
        ds.host,
        ds.day_index,
        ds.hit_count,
        ds.unique_visitors
    FROM
        daily_stats ds
)
-- Step 3: Upsert into host_activity_reduced
INSERT INTO host_activity_reduced (month, host, hit_array, unique_visitors_array)
SELECT
    ud.month_start AS month,
    ud.host,
    -- Initialize hit_array with zeros up to the previous day, then set hit_count at day_index
    ARRAY_FILL(0, ARRAY[ud.day_index - 1]) || ARRAY[ud.hit_count] AS hit_array,
    -- Initialize unique_visitors_array similarly
    ARRAY_FILL(0, ARRAY[ud.day_index - 1]) || ARRAY[ud.unique_visitors] AS unique_visitors_array
FROM
    upsert_data ud
ON CONFLICT (month, host) DO UPDATE
SET
    hit_array = host_activity_reduced.hit_array || EXCLUDED.hit_array,
    unique_visitors_array = host_activity_reduced.unique_visitors_array || EXCLUDED.unique_visitors_array;
