from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

def clean_flights_logic(df):
    
    # ---------------------------------------------------------
    # 1. GESTION DE LA DATE (FlightDate)
    # ---------------------------------------------------------
    ts_col_name = df.columns[0] # La colonne long du début
    
    df = df.withColumn(
        "FlightDate",
        (F.col(ts_col_name) / 1000000).cast(TimestampType()).cast("date")
    )
    
    # Reconstruction des éléments de date
    df = df.withColumn("Year", F.year("FlightDate")) \
           .withColumn("Month", F.month("FlightDate")) \
           .withColumn("DayOfMonth", F.dayofmonth("FlightDate")) \
           .withColumn("DayOfWeek", F.dayofweek("FlightDate"))

    # ---------------------------------------------------------
    # 2. GESTION DES HEURES (PREVUES vs REELLES)
    # ---------------------------------------------------------
    
    # On identifie les paires (NomColonneBrute, NomColonneFinale)
    # CRSDepTime = Prévu / DepTime = Réel
    time_cols_map = [
        ("CRSDepTime", "ScheduledDepTime"),
        ("CRSArrTime", "ScheduledArrTime"),
        ("DepTime", "ActualDepTime"),     # <-- Ajout du temps réel
        ("ArrTime", "ActualArrTime")      # <-- Ajout de l'arrivée réelle
    ]

    for raw_col, new_col in time_cols_map:
        if raw_col in df.columns:
            # Conversion du format long/int bizarre en Timestamp complet
            # On passe par une étape intermédiaire pour extraire l'heure "HH:mm:ss"
            # car le long raw est basé sur l'année 1899
            
            # 1. Cast en timestamp (donne une date en 1899)
            # Certains fichiers ont des INT (1430), d'autres des LONG (timestamp 1899). 
            # On gère le cas LONG comme vu précédemment.
            df = df.withColumn(f"temp_{new_col}", (F.col(raw_col) / 1000000).cast(TimestampType()))
            
            # 2. Extraction HH:mm:ss
            df = df.withColumn(f"time_str_{new_col}", F.date_format(F.col(f"temp_{new_col}"), "HH:mm:ss"))
            
            # 3. Collage sur la vraie FlightDate
            df = df.withColumn(
                new_col,
                F.to_timestamp(
                    F.concat(F.col("FlightDate").cast("string"), F.lit(" "), F.col(f"time_str_{new_col}")),
                    "yyyy-MM-dd HH:mm:ss"
                )
            )
            df = df.drop(f"temp_{new_col}", f"time_str_{new_col}")
        else:
            # Si la colonne n'existe pas (ex: ActualDepTime manquant pour un vol annulé), on met null
            df = df.withColumn(new_col, F.lit(None).cast("timestamp"))

    # ---------------------------------------------------------
    # 3. CALCUL / RECUPERATION DES DELAIS (DepDelay / ArrDelay)
    # ---------------------------------------------------------
    
    # --- ArrDelay ---
    if "ArrDelay" in df.columns:
        df = df.withColumn("ArrDelay", F.col("ArrDelay").cast("int"))
    
    # --- DepDelay (Logique Fallback Intelligente) ---
    # 1. On cherche la colonne explicite
    if "DepDelay" in df.columns:
        df = df.withColumn("DepDelay", F.col("DepDelay").cast("int"))
    elif "DEP_DELAY" in df.columns:
        df = df.withColumn("DepDelay", F.col("DEP_DELAY").cast("int"))
    else:
        # 2. Si pas de colonne, on CALCULE : (Actual - Scheduled) en minutes
        # Note : unix_timestamp donne des secondes, on divise par 60
        df = df.withColumn(
            "DepDelay",
            ((F.unix_timestamp("ActualDepTime") - F.unix_timestamp("ScheduledDepTime")) / 60).cast("int")
        )

    # ---------------------------------------------------------
    # 4. RENOMMAGES ET NETTOYAGE
    # ---------------------------------------------------------
    rename_map = {
        "UniqueCarrier": "Carrier",
        "FlightNum": "FlightNumber",
        "CRSElapsedTime": "ScheduledElapsedTime",
    }
    for src, dst in rename_map.items():
        if src in df.columns:
            df = df.withColumnRenamed(src, dst)

    # Nettoyage Strings
    for col in ["Origin", "Dest", "TailNum", "Carrier"]:
        if col in df.columns:
            df = df.withColumn(col, F.upper(F.trim(F.col(col))))

    # ---------------------------------------------------------
    # 5. FILTRAGE
    # ---------------------------------------------------------
    condition = (
        F.col("FlightDate").isNotNull() &
        F.col("ScheduledDepTime").isNotNull() & 
        F.col("TailNum").isNotNull() & 
        (F.col("TailNum") != "") & 
        (F.col("TailNum") != "UNKNOW") 
    )
    
    df = df.filter(condition)

    return df

@transform_df(
    Output("/skywisesandbox-5785ef/Flight Data Project/data/intermediate/clean_flights"),
    raw_flights=Input("ri.foundry.main.dataset.14bfb5ea-16b6-4181-9000-67237ff5abcc"),
)
def compute(raw_flights):
    df = clean_flights_logic(raw_flights)
    
    # Limit
    df = df.limit(5000000)
    
    # Sélection finale (On ajoute ActualDepTime pour info)
    keep_cols = [
        "Year", "Month", "DayOfMonth", "DayOfWeek", "FlightDate",
        "Carrier", "FlightNumber", "TailNum", 
        "Origin", "Dest", "Distance",
        "ScheduledDepTime", "ActualDepTime", # Ajouté pour verif
        "ScheduledArrTime", "ActualArrTime",
        "DepDelay", "ArrDelay", "ScheduledElapsedTime"
    ]
    
    final_cols = [c for c in keep_cols if c in df.columns]
    df = df.select(*final_cols)

    # Dedup
    df = df.dropDuplicates(["FlightDate", "Carrier", "FlightNumber", "Origin", "Dest"]) \
           .withColumn("ingestion_ts_utc", F.current_timestamp())

    return df
