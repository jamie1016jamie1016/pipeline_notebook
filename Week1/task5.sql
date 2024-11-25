-- Replace <current_year> with the year you're processing

WITH last_scd AS (
    SELECT *
    FROM actors_history_scd
    WHERE end_date IS NULL
),
current_actors AS (
    SELECT *
    FROM actors
    WHERE current_year = 1971
),
changes AS (
    SELECT
        a.actorid,
        a.quality_class AS new_quality_class,
        a.is_active AS new_is_active,
        s.quality_class AS old_quality_class,
        s.is_active AS old_is_active,
        s.start_date AS scd_start_date
    FROM current_actors a
    LEFT JOIN last_scd s ON a.actorid = s.actorid
),
-- Combine updated_records and new_records into one CTE
combined_records AS (
    -- Records with no changes (updated_records)
    SELECT
        c.actorid,
        c.old_quality_class AS quality_class,
        c.old_is_active AS is_active,
        s.start_date,
        NULL::DATE AS end_date
    FROM changes c
    JOIN last_scd s ON c.actorid = s.actorid
    WHERE c.new_quality_class = c.old_quality_class AND c.new_is_active = c.old_is_active
    UNION ALL
    -- Records with changes (updated_records)
    SELECT
        c.actorid,
        c.old_quality_class AS quality_class,
        c.old_is_active AS is_active,
        s.start_date,
        DATE_TRUNC('year', TO_DATE(1971::TEXT, 'YYYY')) - INTERVAL '1 day' AS end_date
    FROM changes c
    JOIN last_scd s ON c.actorid = s.actorid
    WHERE c.new_quality_class <> c.old_quality_class OR c.new_is_active <> c.old_is_active
    UNION ALL
    -- New records for actors with changes (new_records)
    SELECT
        c.actorid,
        c.new_quality_class AS quality_class,
        c.new_is_active AS is_active,
        DATE_TRUNC('year', TO_DATE(1971::TEXT, 'YYYY')) AS start_date,
        NULL::DATE AS end_date
    FROM changes c
    WHERE c.new_quality_class <> c.old_quality_class OR c.new_is_active <> c.old_is_active
    UNION ALL
    -- New actors not in SCD table (new_records)
    SELECT
        a.actorid,
        a.quality_class,
        a.is_active,
        DATE_TRUNC('year', TO_DATE(1971::TEXT, 'YYYY')) AS start_date,
        NULL::DATE AS end_date
    FROM current_actors a
    LEFT JOIN last_scd s ON a.actorid = s.actorid
    WHERE s.actorid IS NULL
)
-- Single INSERT statement using combined_records
INSERT INTO actors_history_scd (actorid, quality_class, is_active, start_date, end_date)
SELECT actorid, quality_class, is_active, start_date, end_date
FROM combined_records
ON CONFLICT (actorid, start_date) DO UPDATE SET
    end_date = EXCLUDED.end_date;


select * from actors_history_scd order by actorid, start_date;