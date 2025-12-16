from transforms.api import transform_df, Input, Output
from pyspark.sql import functions as F

@transform_df(
    Output("/skywisesandbox-5785ef/Flight Data Project/data/intermediate/flights_enriched"),
    flights=Input("/skywisesandbox-5785ef/Flight Data Project/data/intermediate/clean_flights"),
    airports=Input("ri.foundry.main.dataset.f210195d-2650-4a99-a6b3-66fc2b194ada"),
    carriers=Input("ri.foundry.main.dataset.2734a0d4-d751-4719-9bb9-a70421240290"),
    planes=Input("ri.foundry.main.dataset.d23ae613-bbcc-4853-a17e-c11421e040f7"),
)
def compute(flights, airports, carriers, planes):
    # ---------------------------------------------------------
    # 1. PREPARATION & NETTOYAGE DU DATASET PRINCIPAL
    # ---------------------------------------------------------
    # On supprime les métadonnées techniques
    df = flights.drop("ingestion_ts_utc", "last_updated_ts_utc")
    
    # SUPPRESSION REDONDANCES : On a déjà FlightDate, pas besoin de traîner ces colonnes
    # (Sauf si tu as un besoin très spécifique de partitionnement par année)
    df = df.drop("Year", "Month", "DayOfMonth", "DayOfWeek")
    
    # On capture les colonnes restantes (qui incluent DepDelay, ActualDepTime...)
    flight_cols = df.columns

    # ---------------------------------------------------------
    # 2. PREPARATION DES REFERENCE (Version "Light")
    # ---------------------------------------------------------
    
    # Carriers : On garde le code et le Nom (pour l'affichage lisible)
    ref_carriers = carriers.drop("ingestion_ts_utc") \
        .withColumn("carrier_code", F.trim(F.col("carrier_code"))) \
        .withColumnRenamed("carrier_name", "CarrierName") \
        .select("carrier_code", "CarrierName") # <--- On ne garde que ça

    # Planes : On garde juste le Constructeur et le Modèle pour l'info-bulle
    # On supprime l'année de fabrication qui posait problème (full null)
    ref_planes = planes.drop("ingestion_ts_utc") \
        .withColumn("TailNum", F.trim(F.col("TailNum"))) \
        .withColumnRenamed("Manufacturer", "PlaneManufacturer") \
        .withColumnRenamed("Model", "PlaneModel") \
        .select("TailNum", "PlaneManufacturer", "PlaneModel") # <--- Selection stricte

    # Airports : On ne garde QUE le nom de l'aéroport pour l'affichage.
    # On vire City, State, Country, Lat, Lon (accessibles via Ontology Link si besoin)
    ref_airports = airports.drop("ingestion_ts_utc") \
        .withColumn("iata", F.trim(F.col("iata"))) \
        .select("iata", "airport") # <--- On ne charge pas le reste

    # ---------------------------------------------------------
    # 3. JOINTURES (ENRICHISSEMENT)
    # ---------------------------------------------------------

    # --- A. Enrichissement Carrier ---
    df = df.join(
        ref_carriers,
        df.Carrier == ref_carriers.carrier_code,
        "left"
    ).drop("carrier_code")

    # --- B. Enrichissement Origin (Nom Aéroport seulement) ---
    origin_cols = ref_airports \
        .withColumnRenamed("iata", "Origin_Key") \
        .withColumnRenamed("airport", "OriginAirportName")
    
    df = df.join(origin_cols, df.Origin == origin_cols.Origin_Key, "left") \
           .drop("Origin_Key")

    # --- C. Enrichissement Destination (Nom Aéroport seulement) ---
    dest_cols = ref_airports \
        .withColumnRenamed("iata", "Dest_Key") \
        .withColumnRenamed("airport", "DestAirportName")
    
    df = df.join(dest_cols, df.Dest == dest_cols.Dest_Key, "left") \
           .drop("Dest_Key")

    # --- D. Enrichissement Planes ---
    df = df.join(ref_planes, "TailNum", "left")

    # ---------------------------------------------------------
    # 4. SELECTION FINALE
    # ---------------------------------------------------------
    
    # Liste explicite des colonnes d'enrichissement qu'on veut garder
    enriched_cols = [
        "CarrierName",
        "OriginAirportName", 
        "DestAirportName",
        "PlaneManufacturer", 
        "PlaneModel"
    ]

    # Combinaison : Colonnes du vol (nettoyées) + Enrichissement
    final_selection = [c for c in flight_cols + enriched_cols if c in df.columns]

    df = df.select(*final_selection)

    # Ajout du timestamp technique final
    return df.withColumn("ingestion_ts_utc", F.current_timestamp())
