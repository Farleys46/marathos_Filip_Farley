USE CATALOG marathos;
USE SCHEMA platinum;

CREATE OR REFRESH MATERIALIZED VIEW marathos.platinum.mart_distance_events
COMMENT 'Mart for distance-based events' AS
SELECT 
    f.result_id,
    f.athlete_performance,
    f.athlete_avg_speed,
    f.athlete_performance_value,
    f.performance_unit,
    a.athlete_id,
    a.athlete_age_category,
    a.athlete_gender,
    a.athlete_country,
    e.event_name,
    e.event_date,
    e.event_id,
    e.event_type,
    e.event_number_of_finishers,
    e.event_country,
    e.distance_in_km,        
    e.distance_category,
    e.event_distance_length
FROM marathos.gold.fct_results f
LEFT JOIN marathos.gold.dim_event e ON f.event_id = e.event_id
LEFT JOIN marathos.gold.dim_athlete a ON f.athlete_id = a.athlete_id
WHERE e.event_type = 'distance';