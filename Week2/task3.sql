-- Task 3: Cumulative Query to Generate device_activity_datelist and datelist_int

WITH date_series AS (
    SELECT
        date,
        ROW_NUMBER() OVER (ORDER BY date) - 1 AS bit_position
    FROM
        generate_series('2023-01-01'::DATE, '2023-01-31'::DATE, INTERVAL '1 day') AS date_series(date)
),
user_activity AS (
    SELECT
        e.user_id,
        d.browser_type,
        DATE(e.event_time) AS event_date
    FROM
        events e
    INNER JOIN
        devices d ON e.device_id = d.device_id
    WHERE
        e.event_time::date >= '2023-01-01'::DATE
        AND e.event_time::date < '2023-02-01'::DATE
        AND e.user_id IS NOT NULL
),
user_activity_aggregated AS (
    SELECT
        ua.user_id,
        ua.browser_type,
        ARRAY_AGG(DISTINCT ua.event_date ORDER BY ua.event_date) AS device_activity_datelist
    FROM
        user_activity ua
    GROUP BY
        ua.user_id,
        ua.browser_type
),
user_bit_positions AS (
    SELECT
        uaa.user_id,
        uaa.browser_type,
        ds.bit_position
    FROM
        user_activity_aggregated uaa
    JOIN
        UNNEST(uaa.device_activity_datelist) AS active_date(event_date)
        ON TRUE
    JOIN
        date_series ds ON active_date.event_date = ds.date
),
user_datelist_int AS (
    SELECT
        ubp.user_id,
        ubp.browser_type,
        SUM(POW(2, ubp.bit_position)::BIGINT) AS datelist_int
    FROM
        user_bit_positions ubp
    GROUP BY
        ubp.user_id,
        ubp.browser_type
)
INSERT INTO user_devices_cumulated (user_id, browser_type, device_activity_datelist, datelist_int)
SELECT
    uaa.user_id,
    uaa.browser_type,
    uaa.device_activity_datelist,
    udi.datelist_int
FROM
    user_activity_aggregated uaa
INNER JOIN
    user_datelist_int udi ON uaa.user_id = udi.user_id AND uaa.browser_type = udi.browser_type
ON CONFLICT (user_id, browser_type) DO UPDATE
SET
    device_activity_datelist = (
        SELECT ARRAY(
            SELECT DISTINCT date_element
            FROM UNNEST(user_devices_cumulated.device_activity_datelist || EXCLUDED.device_activity_datelist) AS date_element
            ORDER BY date_element
        )
    ),
    datelist_int = COALESCE(user_devices_cumulated.datelist_int, 0) | EXCLUDED.datelist_int;

select * from user_devices_cumulated
