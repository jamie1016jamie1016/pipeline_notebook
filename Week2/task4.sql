-- Task 4: Generate datelist_int from device_activity_datelist

-- Step 1: Define the date range and generate date series with bit positions
WITH date_series AS (
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) - 1 AS bit_position
    FROM (
        SELECT DISTINCT unnest(device_activity_datelist) AS date
        FROM user_devices_cumulated
    ) AS dates
),
-- Step 2: Map each date in device_activity_datelist to its bit position
user_bit_positions AS (
    SELECT
        u.user_id,
        u.browser_type,
        ds.bit_position
    FROM
        user_devices_cumulated u
    JOIN
        UNNEST(u.device_activity_datelist) AS active_date(event_date)
        ON TRUE
    JOIN
        date_series ds ON active_date.event_date = ds.date
)
-- Step 3: Calculate datelist_int for each user and browser_type
UPDATE user_devices_cumulated u
SET datelist_int = ubp.datelist_int
FROM (
    SELECT
        ubp.user_id,
        ubp.browser_type,
        SUM(POW(2, ubp.bit_position)::BIGINT) AS datelist_int
    FROM
        user_bit_positions ubp
    GROUP BY
        ubp.user_id,
        ubp.browser_type
) ubp
WHERE
    u.user_id = ubp.user_id AND
    u.browser_type = ubp.browser_type;
