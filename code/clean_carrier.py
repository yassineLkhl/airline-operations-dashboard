from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.window import Window

@transform_df(
    Output("/path/to/clean/carriers"),
    raw=Input("RAW_CARRIERS_RID"),
)
def compute(raw):
    """
    Harmonizes carrier reference data from potentially inconsistent sources.
    
    Key Logic:
    - Flexible Schema Handling: Maps various column naming conventions to a standard schema.
    - Data Quality: Removes non-alphanumeric characters from codes.
    - Intelligent Deduplication: Uses Window functions to keep the most descriptive name 
      for each carrier code (e.g. preferring "American Airlines" over "American").
    """
    df = raw

    # ---------------------------------------------------------
    # 1. SCHEMA STANDARDIZATION
    # ---------------------------------------------------------
    # Handle variations in raw column names (e.g., from different CSV dumps)
    rename_map = {
        "Code": "carrier_code", "CARRIER": "carrier_code", "carrier": "carrier_code", "CarrierCode": "carrier_code",
        "Description": "carrier_name", "CARRIER_NAME": "carrier_name", "name": "carrier_name", "CarrierName": "carrier_name",
    }
    for src, dst in rename_map.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)

    # Safety net: Ensure columns exist
    if "carrier_code" not in df.columns:
        df = df.withColumn("carrier_code", F.lit(None).cast("string"))
    if "carrier_name" not in df.columns:
        df = df.withColumn("carrier_name", F.lit(None).cast("string"))

    # ---------------------------------------------------------
    # 2. CLEANING
    # ---------------------------------------------------------
    df = (
        df
        # Codes: Uppercase, trim, remove non-alphanumeric chars (keep strict IATA format)
        .withColumn("carrier_code", F.upper(F.trim(F.regexp_replace(F.col("carrier_code"), r"[^A-Z0-9]", ""))))
        # Names: Normalize whitespace, handle string representations of NULL
        .withColumn("carrier_name", F.trim(F.col("carrier_name")))
        .withColumn("carrier_name", F.when(F.col("carrier_name").isin("NULL", "N/A", ""), None).otherwise(F.col("carrier_name")))
        .withColumn("carrier_name", F.regexp_replace(F.col("carrier_name"), r"\s+", " "))
    )

    # Filter out invalid codes (IATA/ICAO are usually 2-3 chars)
    df = df.where(F.length(F.col("carrier_code")) >= 2)

    # ---------------------------------------------------------
    # 3. DEDUPLICATION STRATEGY
    # ---------------------------------------------------------
    # Goal: Keep one row per carrier code.
    # Logic: Prioritize the longest name (usually the full official name) over abbreviations.
    w = Window.partitionBy("carrier_code").orderBy(
        F.desc(F.length(F.col("carrier_name"))), 
        F.col("carrier_name").asc_nulls_last()
    )
    
    df = (
        df
        .withColumn("rn", F.row_number().over(w))
        .where(F.col("rn") == 1)
        .drop("rn")
    )

    # ---------------------------------------------------------
    # 4. FINAL OUTPUT
    # ---------------------------------------------------------
    df = (
        df.select(
            F.col("carrier_code"),
            F.col("carrier_name"),
        )
        .withColumn("ingestion_ts_utc", F.current_timestamp())
    )

    return df
