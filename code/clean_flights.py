from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

# ==============================================================
# BUSINESS LOGIC (Decoupled from Foundry for Unit Testing)
# ==============================================================
def clean_flights_logic(df):
    """
    Core cleaning logic for flight data.
    
    Key Data Quality fixes:
    1. FlightDate: specific handling of raw long timestamps (microseconds).
    2. Scheduled Times: Raw source uses an Excel/Lotus epoch (1899) for times, 
       resulting in negative timestamps. We reconstruct proper ISO timestamps.
    3. DepDelay: Implements a fallback calculation (Actual - Scheduled) 
       when the explicit column is missing.
    """
    
    # ---------------------------------------------------------
    # 1. DATE PARSING
    # ---------------------------------------------------------
    # The first column contains the flight date in microseconds
    # We detect it dynamically or use a specific name if known
    ts_col_name = df.columns[0] 
    
    df = df.withColumn(
        "FlightDate",
        (F.col(ts_col_name) / 1000000).cast(TimestampType()).cast("date")
    )
    
    # Extract date parts for partitioning or analysis if needed
    df = df.withColumn("Year", F.year("FlightDate")) \
           .withColumn("Month", F.month("FlightDate")) \
           .withColumn("DayOfMonth", F.dayofmonth("FlightDate")) \
           .withColumn("DayOfWeek", F.dayofweek("FlightDate"))

    # ---------------------------------------------------------
    # 2. TIME RECONSTRUCTION (The "1899 Epoch" Fix)
    # ---------------------------------------------------------
    # Mapping raw columns (int/long) to final standard names
    time_cols_map = [
        ("CRSDepTime", "ScheduledDepTime"),
        ("CRSArrTime", "ScheduledArrTime"),
        ("DepTime", "ActualDepTime"),
        ("ArrTime", "ActualArrTime")
    ]

    for raw_col, new_col in time_cols_map:
        if raw_col in df.columns:
            # Step A: Convert microsecond long to timestamp (often results in 1899-12-30)
            df = df.withColumn(f"temp_{new_col}", (F.col(raw_col) / 1000000).cast(TimestampType()))
            
            # Step B: Extract HH:mm:ss string
            df = df.withColumn(f"time_str_{new_col}", F.date_format(F.col(f"temp_{new_col}"), "HH:mm:ss"))
            
            # Step C: Combine valid FlightDate with extracted Time
            df = df.withColumn(
                new_col,
                F.to_timestamp(
                    F.concat(F.col("FlightDate").cast("string"), F.lit(" "), F.col(f"time_str_{new_col}")),
                    "yyyy-MM-dd HH:mm:ss"
                )
            )
            # Cleanup temp columns
            df = df.drop(f"temp_{new_col}", f"time_str_{new_col}")
        else:
            # Handle missing columns gracefully
            df = df.withColumn(new_col, F.lit(None).cast("timestamp"))

    # ---------------------------------------------------------
    # 3. DELAY CALCULATION (Fallback Logic)
    # ---------------------------------------------------------
    
    # ArrDelay: Cast to integer
    if "ArrDelay" in df.columns:
        df = df.withColumn("ArrDelay", F.col("ArrDelay").cast("int"))
    
    # DepDelay: Critical metric for root cause analysis
    # Strategy: Check explicit column -> Check alias -> Calculate it
    if "DepDelay" in df.columns:
        df = df.withColumn("DepDelay", F.col("DepDelay").cast("int"))
    elif "DEP_DELAY" in df.columns:
        df = df.withColumn("DepDelay", F.col("DEP_DELAY").cast("int"))
    else:
        # Fallback: Calculate difference in minutes
        df = df.withColumn(
            "DepDelay",
            ((F.unix_timestamp("ActualDepTime") - F.unix_timestamp("ScheduledDepTime")) / 60).cast("int")
        )

    # ---------------------------------------------------------
    # 4. STANDARDIZATION
    # ---------------------------------------------------------
    rename_map = {
        "UniqueCarrier": "Carrier",
        "FlightNum": "FlightNumber",
        "CRSElapsedTime": "ScheduledElapsedTime",
    }
    for src, dst in rename_map.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)

    # String cleaning (Trim & Uppercase)
    for col in ["Origin", "Dest", "TailNum", "Carrier"]:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))

    # ---------------------------------------------------------
    # 5. DATA QUALITY FILTERING
    # ---------------------------------------------------------
    # Remove rows where critical business keys are missing
    condition = (
        F.col("FlightDate").isNotNull() &
        F.col("ScheduledDepTime").isNotNull() & 
        F.col("TailNum").isNotNull() & 
        (F.col("TailNum") != "") & 
        (F.col("TailNum") != "UNKNOW") 
    )
    
    return df.filter(condition)


# ==============================================================
# FOUNDRY ORCHESTRATION
# ==============================================================
@transform_df(
    Output("/path/to/clean/clean_flights"),
    raw_flights=Input("RAW_FLIGHTS_DATASET_RID"),
)
def compute(raw_flights):
    # Apply Business Logic
    df = clean_flights_logic(raw_flights)
    
    # ---------------------------------------------------------
    # PRODUCTION SETTINGS
    # ---------------------------------------------------------
    # Limit for development/demo purposes (Full dataset is >100M rows)
    df = df.limit(5000000)
    
    # Final Projection
    keep_cols = [
        "FlightDate", "Carrier", "FlightNumber", "TailNum", 
        "Origin", "Dest", "Distance",
        "ScheduledDepTime", "ActualDepTime",
        "ScheduledArrTime", "ActualArrTime",
        "DepDelay", "ArrDelay", "ScheduledElapsedTime"
    ]
    
    # Select only existing columns to avoid AnalysisException
    final_cols = [c for c in keep_cols if c in df.columns]
    df = df.select(*final_cols)

    # Deduplication based on business keys
    df = df.dropDuplicates(["FlightDate", "Carrier", "FlightNumber", "Origin", "Dest"]) \
           .withColumn("ingestion_ts_utc", F.current_timestamp())

    return df
