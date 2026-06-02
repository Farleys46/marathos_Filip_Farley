CREATE OR REFRESH STREAMING TABLE marathos.gold.fct_results
COMMENT "Fact table - gold layer" AS
SELECT
    result_id,
    event_id,
    athlete_id,
    athlete_performance,
    athlete_performance_value,
    performance_unit,
    athlete_average_speed AS athlete_avg_speed
FROM 
  STREAM marathos.silver.cleaned_marathos;




