WITH deduped_game_details AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY game_id, team_id, player_id
        ) AS rownum
    FROM
        game_details
)
SELECT
    *
FROM
    deduped_game_details
WHERE
    rownum = 1;
