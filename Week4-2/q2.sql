SELECT
    gd.player_name,
    gd.team_abbreviation,
    ps.season,
    SUM(gd.pts) AS total_points,
    COUNT(DISTINCT gd.game_id) AS games_played,
    GROUPING(gd.player_name, gd.team_abbreviation, ps.season) AS grouping_level
FROM 
    game_details gd
LEFT JOIN 
    player_seasons ps
ON 
    gd.player_name = ps.player_name
GROUP BY 
    GROUPING SETS (
        (gd.player_name, gd.team_abbreviation), -- Player and Team
        (gd.player_name, ps.season),            -- Player and Season
        (gd.team_abbreviation)                 -- Team
    )
ORDER BY 
    grouping_level DESC, total_points DESC;