from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F

@transform_df(
    Output("/path/to/clean/planes"),
    raw=Input("RAW_PLANES_RID"),
)
def compute(raw):
    """
    Cleans aircraft fleet reference data.
    
    Key Features:
    - Standardization of column names.
    - TailNum validation (Primary Key).
    - Data Quality Check: Filters out aberrant manufacturing years (future dates or pre-1900).
    """
    df = raw

    # 1. Standardize Column Names
    rename_map = {
        "tailnum": "TailNum",
        "manufacturer": "Manufacturer",
        "model": "Model",
        "year": "YearManufactured",
        "type": "AircraftType",
    }
    for src, dst in rename_map.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)

    # 2. Clean Primary Key (TailNum)
    df = (
        df.withColumn("TailNum", F.upper(F.trim(F.col("TailNum"))))
          .where(F.col("TailNum").isNotNull() & (F.col("TailNum") != ""))
    )

    # 3. Year Validation (Data Quality)
    if "YearManufactured" in df.columns:
        df = df.withColumn("YearManufactured", F.col("YearManufactured").cast("int"))

        # Logical filter: Year must be between 1900 and Current Year
        # Eliminates data entry errors (e.g. year 0, year 9999)
        df = df.where(
            (F.col("YearManufactured") >= 1900) &
            (F.col("YearManufactured") <= F.year(F.current_date()))
        )

    # 4. Final Projection
    keep_cols = [
        "TailNum",
        "Manufacturer",
        "Model",
        "AircraftType",
        "YearManufactured"
    ]
    # Dynamic selection to avoid errors if raw schema changes slightly
    df = df.select([c for c in keep_cols if c in df.columns])

    df = df.withColumn("ingestion_ts_utc", F.current_timestamp())

    return df
