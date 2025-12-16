from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F

@transform_df(
    Output("/path/to/clean/flights_enriched"),
    flights=Input("RID_CLEAN_FLIGHTS"),
    airports=Input("RID_CLEAN_AIRPORTS"),
    carriers=Input("RID_CLEAN_CARRIERS"),
    planes=Input("RID_CLEAN_PLANES"),
)
def compute(flights, airports, carriers, planes):
    """
    Enriches the main flight dataset with dimensional data (Carriers, Airports, Planes).
    
    Architecture Pattern: Star Schema / Hub & Spoke
    - Fact Table: Flights
    - Dimension Tables: Carriers, Airports, Planes
    
    Optimization:
    - Redundant columns are dropped to keep the schema normalized.
    - Reference datasets are projected (selected) to keep only display-relevant columns.
    - Broadcast joins are used for small dimension tables to optimize shuffle operations.
    """

    # ---------------------------------------------------------
    # 1. FACT TABLE PREPARATION
    # ---------------------------------------------------------
    # Drop technical metadata columns
    df = flights.drop("ingestion_ts_utc", "last_updated_ts_utc")
    
    # SCHEMA OPTIMIZATION: Drop redundant time columns (Year, Month...) 
    # since 'FlightDate' provides the full date context.
    df = df.drop("Year", "Month", "DayOfMonth", "DayOfWeek")
    
    # Capture columns from the fact table before joins
    flight_cols = df.columns

    # ---------------------------------------------------------
    # 2. DIMENSION TABLES PREPARATION
    # ---------------------------------------------------------
    # We apply "Projection" here: selecting only the columns needed for the UI/Dashboard.
    # Detailed attributes (City, Geo, etc.) remain in the Ontology Objects.

    # Carriers: Keep Code and Name
    ref_carriers = carriers.drop("ingestion_ts_utc") \
        .withColumn("carrier_code", F.trim(F.col("carrier_code"))) \
        .withColumnRenamed("carrier_name", "CarrierName") \
        .select("carrier_code", "CarrierName")

    # Planes: Keep Manufacturer and Model
    ref_planes = planes.drop("ingestion_ts_utc") \
        .withColumn("TailNum", F.trim(F.col("TailNum"))) \
        .withColumnRenamed("Manufacturer", "PlaneManufacturer") \
        .withColumnRenamed("Model", "PlaneModel") \
        .select("TailNum", "PlaneManufacturer", "PlaneModel")

    # Airports: Keep Code and Name only
    ref_airports = airports.drop("ingestion_ts_utc") \
        .withColumn("iata", F.trim(F.col("iata"))) \
        .select("iata", "airport")

    # ---------------------------------------------------------
    # 3. ENRICHMENT (JOINS)
    # ---------------------------------------------------------
    # Note: We use Left Joins to preserve all flights even if metadata is missing.
    # We use F.broadcast() on dimension tables as they are small enough to fit in memory,
    # preventing expensive shuffle operations across the cluster.

    # --- A. Carrier Enrichment ---
    df = df.join(
        F.broadcast(ref_carriers),
        df.Carrier == ref_carriers.carrier_code,
        "left"
    ).drop("carrier_code")

    # --- B. Origin Airport Enrichment ---
    origin_cols = ref_airports \
        .withColumnRenamed("iata", "Origin_Key") \
        .withColumnRenamed("airport", "OriginAirportName")
    
    df = df.join(
        F.broadcast(origin_cols), 
        df.Origin == origin_cols.Origin_Key, 
        "left"
    ).drop("Origin_Key")

    # --- C. Destination Airport Enrichment ---
    dest_cols = ref_airports \
        .withColumnRenamed("iata", "Dest_Key") \
        .withColumnRenamed("airport", "DestAirportName")
    
    df = df.join(
        F.broadcast(dest_cols), 
        df.Dest == dest_cols.Dest_Key, 
        "left"
    ).drop("Dest_Key")

    # --- D. Plane Enrichment ---
    # Planes table might be larger, broadcast is conditional here depending on fleet size.
    # Assuming standard commercial fleet, it fits in memory.
    df = df.join(
        F.broadcast(ref_planes), 
        "TailNum", 
        "left"
    )

    # ---------------------------------------------------------
    # 4. FINAL SELECTION & METADATA
    # ---------------------------------------------------------
    
    # Define explicit enrichment columns to ensure correct ordering
    enriched_cols = [
        "CarrierName",
        "OriginAirportName", 
        "DestAirportName",
        "PlaneManufacturer", 
        "PlaneModel"
    ]

    # Combine original flight columns + new enriched columns
    final_selection = [c for c in flight_cols + enriched_cols if c in df.columns]

    df = df.select(*final_selection)

    # Add technical timestamp for lineage tracking
    return df.withColumn("ingestion_ts_utc", F.current_timestamp())
