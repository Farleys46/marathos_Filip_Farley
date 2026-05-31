import re
from pyspark.sql.functions import (
    col,
    expr,
    lit,
    lower,
    regexp_extract,
    regexp_replace,
    round,
    size,
    split,
    to_timestamp,
    trim,
    when
)
# Läs in data
df = spark.sql("SELECT * FROM marathos.bronze.raw_marathos")

# Byt namn på kolumner t.ex "maratos_namn"
def to_snake_case(name):
    return re.sub(r"[\s]+", "_", name.strip().casefold())

def rename_colummns_to_snake_case(df):
    new_columns = [to_snake_case(column) for column in df.columns]
    return df.toDF(*new_columns)

df_cleaned = rename_colummns_to_snake_case(df)

# Ändra datum till "datetype" och ta bort datum som inte är "datetype"
df_cleaned = df_cleaned.withColumn(
    "event_dates_clean", 
    regexp_replace(col("event_dates"), r"\.?-[0-9]+", "")
)

df_cleaned = df_cleaned.withColumn(
    "event_date", 
    expr("try_to_date(event_dates_clean, 'dd.MM.yyyy')")
)

# athlete_year_of_birth är nu en int istället för double. 
df_cleaned = df_cleaned.withColumn(
    "athlete_year_of_birth",
    col("athlete_year_of_birth").cast("int")
)

# Byt ut kommatecken mot punkter och kör TRY_CAST för att säkert göra om det till Double
df_cleaned = df_cleaned.withColumn(
    "athlete_average_speed",
    expr("try_cast(regexp_replace(athlete_average_speed, ',', '.') AS DOUBLE)")
)

# Skapa "event_country" baserat på "event_name" sista parentes. 
df_cleaned = df_cleaned.withColumn(
    "event_country",
    trim(regexp_extract(col("event_name"), r"\(([^)]+)\)\s*$", 1))
)




#________________________________________
#_________FILTER OCH TVÄTTNING___________
#________________________________________

df_cleaned = df_cleaned.filter(
    (col("athlete_average_speed") > 0) & 
    (col("athlete_average_speed") <= 25)
)

