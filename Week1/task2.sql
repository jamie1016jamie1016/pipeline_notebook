
WITH yesterday AS (
    SELECT *
    FROM actors
    WHERE current_year = 1970
),
today AS (
    SELECT
        actorid,
        ARRAY_AGG(ROW(film, votes, rating, filmid)::films) AS films_array,
        AVG(rating) AS avg_rating,
        year AS current_year
    FROM actor_films
    WHERE year = 1971
    GROUP BY actorid, year
)
INSERT INTO actors (actorid, films, quality_class, is_active, current_year)
SELECT
    COALESCE(t.actorid, y.actorid) AS actorid,
    CASE
        WHEN y.films IS NULL THEN t.films_array
        WHEN t.films_array IS NOT NULL THEN y.films || t.films_array
        ELSE y.films
    END AS films,
    CASE
        WHEN t.avg_rating IS NOT NULL THEN
            CASE
                WHEN t.avg_rating > 8 THEN 'star'
                WHEN t.avg_rating > 7 THEN 'good'
                WHEN t.avg_rating > 6 THEN 'average'
                ELSE 'bad'
            END::quality_class
        ELSE y.quality_class
    END AS quality_class,
    (t.films_array IS NOT NULL) AS is_active,
    COALESCE(t.current_year, y.current_year + 1) AS current_year
FROM today t
FULL OUTER JOIN yesterday y
ON t.actorid = y.actorid
ON CONFLICT (actorid) DO UPDATE SET
    films = EXCLUDED.films,
    quality_class = EXCLUDED.quality_class,
    is_active = EXCLUDED.is_active,
    current_year = EXCLUDED.current_year;
