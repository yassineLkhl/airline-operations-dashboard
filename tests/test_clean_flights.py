import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType, IntegerType
from code.clean_flights import clean_flights_logic

# Ce test simule un environnement Spark local
# Il permet de valider la logique sans avoir besoin de Foundry

class TestCleanFlights:
    
    @pytest.fixture(scope="module")
    def spark_session(self):
        return SparkSession.builder \
            .master("local[2]") \
            .appName("test-airline-pipeline") \
            .getOrCreate()

    def test_clean_flights_logic(self, spark_session):
        # 1. SETUP : Création d'un jeu de données MOCK
        # On simule le problème du timestamp "1899" et l'absence de colonne DepDelay
        schema = StructType([
            StructField("Year_Month_Day_Micros", LongType(), True), # Date en microsecondes
            StructField("Carrier", StringType(), True),
            StructField("FlightNum", LongType(), True),
            StructField("Origin", StringType(), True),
            StructField("Dest", StringType(), True),
            StructField("TailNum", StringType(), True),
            # Colonnes de temps brutes (Entier ou Long selon la source, ici on teste le Long 1899)
            StructField("CRSDepTime", LongType(), True), # Scheduled Dep
            StructField("DepTime", LongType(), True),    # Actual Dep
            StructField("ArrDelay", IntegerType(), True)
        ])

        # Données de test :
        # - Date : 561168000000000 (approx Oct 1987)
        # - Heure Prévue : -220895550000000 (Correspond à ~14:45 en 1899)
        # - Heure Réelle : -220895490000000 (Correspond à ~14:55 en 1899) -> Retard de 10 min
        data = [
            (
                561168000000000, "AA", 101, " JFK ", "LHR", "N101AA", 
                -220895550000000, # Scheduled (~14:45)
                -220895490000000, # Actual (~14:55)
                5
            ),
            # Ligne invalide (TailNum manquant) pour tester le filtre
            (561168000000000, "BA", 202, "LHR", "CDG", None, -220895550000000, -220895550000000, 0)
        ]

        input_df = spark_session.createDataFrame(data, schema)

        # 2. EXECUTE : On lance la logique de nettoyage
        result_df = clean_flights_logic(input_df)

        # 3. ASSERT : Vérifications
        
        # A. Vérifier que le filtrage fonctionne (la ligne sans TailNum doit disparaître)
        assert result_df.count() == 1, "Le filtrage des données invalides a échoué"
        
        row = result_df.first()

        # B. Vérifier le nettoyage des Strings (Trim)
        assert row["Origin"] == "JFK", "Le trim sur Origin n'a pas fonctionné"

        # C. Vérifier la construction de la Date (Transformation du Long en Date)
        # 561168000000000 correspond au 13 Octobre 1987
        assert str(row["FlightDate"]) == "1987-10-13", f"Erreur de date : {row['FlightDate']}"

        # D. Vérifier la reconstruction de l'heure (Fix du bug 1899)
        # On vérifie que l'heure est bien présente dans le timestamp
        assert "14:45:00" in str(row["ScheduledDepTime"]), "Erreur reconstruction ScheduledDepTime"
        
        # E. Vérifier le calcul automatique du DepDelay (Fallback)
        # 14:55 (Actual) - 14:45 (Scheduled) = 10 minutes
        # Note : Le script calcule ((Actual - Scheduled) / 60)
        assert row["DepDelay"] == 10, f"Le calcul du DepDelay est incorrect. Attendu 10, reçu {row['DepDelay']}"

        print("Tous les tests sont passés avec succès !")
