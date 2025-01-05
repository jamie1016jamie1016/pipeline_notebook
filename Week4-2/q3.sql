-- Q3-1

WITH team_game_results AS (
    SELECT 
        team_abbreviation,
        game_id,
        CASE 
            WHEN pts > (SELECT MAX(pts) FROM game_details g2 WHERE g1.game_id = g2.game_id AND g1.team_abbreviation != g2.team_abbreviation) THEN 1
            ELSE 0
        END AS win
    FROM game_details g1
),
team_rolling_wins AS (
    SELECT 
        team_abbreviation,
        SUM(win) OVER (PARTITION BY team_abbreviation ORDER BY game_id ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_in_last_90
    FROM team_game_results
)
SELECT 
    team_abbreviation,
    MAX(wins_in_last_90) AS max_wins_in_90_games
FROM 
    team_rolling_wins
GROUP BY 
    team_abbreviation;

--Q3-2
WITH lebron_scores AS (
    SELECT 
        game_id,
        player_name,
        CASE WHEN pts > 10 THEN 1 ELSE 0 END AS scored_over_10
    FROM game_details
    WHERE player_name = 'LeBron James'
),
streaks AS (
    SELECT 
        game_id,
        player_name,
        scored_over_10,
        SUM(CASE WHEN scored_over_10 = 0 THEN 1 ELSE 0 END) OVER (PARTITION BY player_name ORDER BY game_id) AS streak_id
    FROM lebron_scores
)
SELECT 
    player_name,
    MAX(COUNT(game_id)) OVER (PARTITION BY player_name, streak_id) AS longest_streak
FROM streaks
WHERE scored_over_10 = 1
GROUP BY player_name, streak_id;

