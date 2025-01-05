
SELECT 
    host, 
    AVG(event_count) AS avg_events_per_session
FROM 
    w4_sessionized_events
WHERE 
    host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
GROUP BY 
    host;
