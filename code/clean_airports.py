from transforms.api import transform_df, Input, Output
import pyspark.sql.functions as F

@transform_df(
    Output("/path/to/clean/airports"),
    airports_raw=Input("RAW_AIRPORTS_RID"),
)
def compute(airports_raw):
    """
    Standardizes airport reference data.
    
    Transformation steps:
    1. Filter invalid entries (missing IATA codes).
    2. Normalize codes to uppercase.
    3. Select and rename geolocation columns for map visualizations.
    """
    df = (
        airports_raw
        # Data Quality: IATA code is the primary key, strictly required
        .dropna(subset=["iata"])
        .withColumn("iata", F.upper(F.col("iata")))
        
        # Projection: Keep only metadata relevant for Dashboard maps
        .select(                                       
            "iata",
            "airport", 
            "city", 
            "state", 
            "country",
            "lat", 
            "long"
        )
        .withColumnRenamed("lat", "latitude")
        .withColumnRenamed("long", "longitude")
    )
    return df
