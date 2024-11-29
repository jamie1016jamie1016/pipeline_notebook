-- Task 6: Incremental Query to Generate host_activity_datelist

-- Step 1: Extract activity data from events for the specific date
WITH new_host_activity AS (
    SELECT
        e.host,
        ARRAY_AGG(DISTINCT DATE(e.event_time)) AS activity_dates
    FROM
        events e
    WHERE
        e.event_time::date >= '2023-01-01'::DATE
        AND e.event_time::date < '2023-01-02'::DATE
        AND e.host IS NOT NULL
    GROUP BY
        e.host
)
-- Step 2: Upsert into hosts_cumulated
INSERT INTO hosts_cumulated (host, host_activity_datelist)
SELECT
    nha.host,
    nha.activity_dates
FROM
    new_host_activity nha
ON CONFLICT (host) DO UPDATE
SET
    host_activity_datelist = (
        SELECT ARRAY(
            SELECT DISTINCT date_element
            FROM UNNEST(hosts_cumulated.host_activity_datelist || EXCLUDED.host_activity_datelist) AS date_element
            ORDER BY date_element
        )
    );
