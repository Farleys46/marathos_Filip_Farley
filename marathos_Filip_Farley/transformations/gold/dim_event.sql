CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_event
COMMENT "Dim Event table - gold layer" AS
SELECT
    event_id,
    MAX_BY(event_type, event_date) AS event_type,
    MAX_BY(event_name, event_date) AS event_name,
    MAX_BY(event_date, event_date) AS event_date,
    MAX_BY(year_of_event, event_date) AS year_of_event,
    MAX_BY(event_country, event_date) AS event_country,
    MAX_BY(event_distance_length, event_date) AS event_distance_length,
    MAX_BY(event_distance_unit, event_date) AS event_distance_unit,
    MAX_BY(event_number_of_finishers, event_date) AS event_number_of_finishers
FROM 
    marathos.silver.cleaned_marathos
GROUP BY event_id;