SELECT 
    AVG(event_count) AS avg_events_per_session
FROM 
    w4_sessionized_events
WHERE 
    host LIKE '%techcreator.io';