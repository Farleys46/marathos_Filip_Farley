import re
from pyspark import pipelines as dp
from utils.utils import rename_columns_to_snake_case
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
    when,
    monotonically_increasing_id,
    hash,
    abs
)

@dp.table(
    name="marathos.silver.cleaned_marathos", 
    comment="Cleaned data for silver layer",
    table_properties={
        "delta.columnMapping.mode": "name",
        "delta.minReaderVersion": "2",
        "delta.minWriterVersion": "5"
        }
)

# Läs in data
def cleaned_marathos():
    df_cleaned = rename_columns_to_snake_case(spark.sql("SELECT * FROM STREAM marathos.bronze.raw_marathos"))

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

    # Ta bort citat tecken från "event_name" eftersom de spökar till det för "event_country"
    df_cleaned = df_cleaned.withColumn(
        "event_name",
        regexp_replace(col("event_name"), '"', "")
    )

    # Skapa "event_country" baserat på "event_name" sista parentes. 
    df_cleaned = df_cleaned.withColumn(
        "event_country",
        trim(regexp_extract(col("event_name"), r"\(([^)]+)\)\s*$", 1))
    )

    # Tvätta bort stjärnor (*) i början av klubbnamnen
    df_cleaned = df_cleaned.withColumn(
        "athlete_club",
        trim(regexp_replace(col("athlete_club"), r"^\*\s*", ""))
    )

    #-------------------------------
    # Städa "event_distance/length"
    #-------------------------------
    df_cleaned = df_cleaned.filter(~col("event_distance/length").contains("/"))
    df_cleaned = df_cleaned.withColumn(
        "event_distance/length", 
        expr("regexp_replace(`event_distance/length`, ',', '.')")
    )

    df_cleaned = df_cleaned.withColumn(
        "raw_unit", 
        lower(trim(regexp_replace(col("event_distance/length"), r"[0-9\.]", "")))
    )

    df_cleaned = df_cleaned.withColumn(
        "event_distance_unit",
        when(col("raw_unit").isin(["miles", "mile", "mi+", "mi"]), "mi")
        .when(col("raw_unit").isin(["k", "km"]), "km")
        .when(col("raw_unit").isin([":h", "h"]), "h")
        .otherwise("invalid") 
    )

    # Kasta bort allt invalid
    df_cleaned = df_cleaned.filter(col("event_distance_unit") != "invalid")

    # Skapa ny kolumn "event_type" så antingen "distance" eller "time"
    df_cleaned = df_cleaned.withColumn(
        "event_type",
        when(col("event_distance_unit").isin(["km", "mi"]), "distance")
        .otherwise("time") 
    )

    # Byt namn på kolumnen för att ta bort "/" ser snyggare ut
    df_cleaned = df_cleaned.withColumnRenamed("event_distance/length", "event_distance_length")
    #-------------------------------------------------------------------------------


    #-------------------------------
    # Städa "athlete_performance"
    #-------------------------------
    df_cleaned = df_cleaned.withColumn(
        "athlete_performance_clean", 
        trim(regexp_replace(regexp_replace(col("athlete_performance"), ",", "."), r"[a-zA-Z\s]", ""))
    )

    df_cleaned = df_cleaned.withColumn(
        "time_array", 
        split(col("athlete_performance_clean"), ":")
    )

    # Plockar ut timmarna, minuter å sekunder
    hours = col("time_array").getItem(0).cast("double")
    minutes = when(size(col("time_array")) > 1, col("time_array").getItem(1).cast("double")).otherwise(lit(0.0))
    seconds = when(size(col("time_array")) > 2, col("time_array").getItem(2).cast("double")).otherwise(lit(0.0))

    df_cleaned = df_cleaned.withColumn(
        "athlete_performance_value",
        when(
            col("athlete_performance_clean").contains(":"),
            round(hours + (minutes / 60) + (seconds / 3600), 2)
        ).otherwise(
            round(col("athlete_performance_clean").cast("double"), 2)
        )
    )

    df_cleaned = df_cleaned.drop("athlete_performance_clean", "time_array")


    df_cleaned = df_cleaned.withColumn(
        "performance_unit",
        when(col("event_distance_unit").isin(["km", "mi"]), "h")
        .when(col("athlete_performance").contains("mi"), "mi")
        .otherwise("km") 
    )
    #-----------------------------------------------------------------------


    #________________________________________
    #_________FILTER OCH TVÄTTNING___________
    #________________________________________

    # Kontrollera att tidslopp har distans-resultat, och tvärtom
    df_cleaned = df_cleaned.withColumn(
        "is_valid_performance",
        when(
            col("event_distance_unit") == "h", 
            col("athlete_performance").endswith("km") | col("athlete_performance").endswith("mi")
        ).otherwise(
            col("athlete_performance").contains(":") | col("athlete_performance").endswith("h")
        )
    )

    # Kasta ut de rader som inte följer logiken (behåll bara True)
    df_cleaned = df_cleaned.filter(col("is_valid_performance") == True)

    # Städa bort hjälpkolumnen
    df_cleaned = df_cleaned.drop("is_valid_performance")

    # Kollade upp average pace rekord i maraton och filtrerar så det inte finns några felaktiga värden
    df_cleaned = df_cleaned.filter(
        (col("athlete_average_speed") > 0) & 
        (col("athlete_average_speed") <= 25)
    )

    #___________________________
    #______Nulls hantering______
    #___________________________

    all_columns = df_cleaned.columns

    # Cleara alla nulls förutom "athlete club" eftersom det är ok att springa utan en klubb
    columns_to_clear = [col_name for col_name in all_columns if col_name != "athlete_club"]

    df_cleaned = df_cleaned.dropna(subset=columns_to_clear)

    #______________________________________
    #______Ta bort temporära columner______
    #______________________________________

    columns_to_drop = [
        "event_dates",  # Originaldatumet
        "event_dates_clean",
        "raw_unit"
    ]

    df_cleaned = df_cleaned.drop(*columns_to_drop)
    #____________________________________________________________

    #--------------------
    #-----Skapa IDs------
    #--------------------

    # Event id som är unik per event och datum
    df_cleaned = df_cleaned.withColumn(
        "event_id", 
        abs(hash(col("event_name"), col("event_date")))
    )
    # result ID som som gör alla resultat unika
    df_cleaned = df_cleaned.withColumn(
        "result_id", 
        abs(hash(expr("uuid()")))
    )
    return df_cleaned