INSERT INTO actors_history_scd (actorid, quality_class, is_active, start_date, end_date)
WITH actors_ordered AS (
    SELECT
        actorid,
        current_year,
        quality_class,
        is_active,
        LAG(quality_class) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_quality_class,
        LAG(is_active) OVER (PARTITION BY actorid ORDER BY current_year) AS previous_is_active
    FROM actors
),
change_flags AS (
    SELECT
        *,
        CASE
            WHEN quality_class <> previous_quality_class OR is_active <> previous_is_active THEN 1
            ELSE 0
        END AS change_indicator
    FROM actors_ordered
),
streaks AS (
    SELECT
        *,
        SUM(change_indicator) OVER (PARTITION BY actorid ORDER BY current_year) AS streak_id
    FROM change_flags
),
streaks_grouped AS (
    SELECT
        actorid,
        quality_class,
        is_active,
        streak_id,
        MIN(DATE_TRUNC('year', TO_DATE(current_year::TEXT, 'YYYY'))) AS start_date
    FROM streaks
    GROUP BY actorid, quality_class, is_active, streak_id
),
streaks_with_end_date AS (
    SELECT
        actorid,
        quality_class,
        is_active,
        start_date,
        LEAD(start_date) OVER (PARTITION BY actorid ORDER BY start_date) - INTERVAL '1 day' AS end_date
    FROM streaks_grouped
)
SELECT
    actorid,
    quality_class,
    is_active,
    start_date,
    end_date
FROM streaks_with_end_date
ORDER BY actorid, start_date;
