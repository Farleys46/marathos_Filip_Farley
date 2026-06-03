USE CATALOG marathos;
USE SCHEMA platinum;

CREATE OR REFRESH MATERIALIZED VIEW marathos.platinum.mart_time_events
COMMENT 'Mart for time-based events' AS
SELECT 
    f.result_id,
    f.athlete_performance,
    f.athlete_avg_speed,
    a.athlete_id,
    a.athlete_age_category,
    a.athlete_gender,
    a.athlete_country,
    e.event_name,
    e.event_date,
    e.event_country,
    e.event_distance_length
FROM marathos.gold.fct_results f
LEFT JOIN dim_event e ON f.event_id = e.event_id
LEFT JOIN dim_athlete a ON f.athlete_id = a.athlete_id
WHERE e.event_type = 'time';