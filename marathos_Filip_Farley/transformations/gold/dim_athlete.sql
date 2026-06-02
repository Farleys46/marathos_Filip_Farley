CREATE OR REFRESH MATERIALIZED VIEW marathos.gold.dim_athlete
COMMENT 'Dim Athlete table - gold layer' AS
SELECT
    athlete_id,
    MAX_BY(athlete_year_of_birth, event_date) AS athlete_year_of_birth,
    MAX_BY(athlete_gender, event_date) AS athlete_gender,
    MAX_BY(athlete_age_category, event_date) AS athlete_age_category,
    MAX_BY(athlete_country, event_date) AS athlete_country,
    MAX_BY(athlete_club, event_date) AS athlete_club
FROM 
    marathos.silver.cleaned_marathos
GROUP BY 
    athlete_id;