SELECT
    player_name,
    CASE
        WHEN is_active = TRUE AND start_season = current_season THEN 'New'
        WHEN is_active = FALSE AND end_season = current_season THEN 'Retired'
        WHEN is_active = TRUE AND start_season < current_season AND end_season IS NULL THEN 'Continued Playing'
        WHEN is_active = TRUE AND start_season > end_season THEN 'Returned from Retirement'
        WHEN is_active = FALSE AND end_season < current_season THEN 'Stayed Retired'
        ELSE 'Unknown'
    END AS state
FROM players_scd;
