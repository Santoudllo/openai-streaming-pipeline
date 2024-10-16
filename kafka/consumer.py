from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Créez une session Spark
spark = SparkSession.builder \
    .appName("KafkaToSpark") \
    .getOrCreate()

# Configurez votre consommateur Kafka
consumer = KafkaConsumer(
    'circulation',  # Remplacez par votre topic
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='your_group_id',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
)

# Définir le schéma pour le DataFrame
schema = StructType([
    StructField("identifiant", StringType(), True),
    StructField("identifiant_ctv", StringType(), True),
    StructField("cp_arrondissement", StringType(), True),
    StructField("numero_stv", IntegerType(), True),
    StructField("typologie", IntegerType(), True),
    StructField("maitre_ouvrage", StringType(), True),
    StructField("objet", StringType(), True),
    StructField("description", StringType(), True),
    StructField("voie", StringType(), True),
    StructField("precision_localisation", StringType(), True),
    StructField("date_debut", StringType(), True),
    StructField("date_fin", StringType(), True),
    StructField("impact_circulation", StringType(), True),
    StructField("impact_circulation_detail", StringType(), True),
    StructField("niveau_perturbation", IntegerType(), True),
    StructField("statut", IntegerType(), True),
    StructField("url_lic", StringType(), True),
    StructField("geo_shape", StringType(), True),  # Considéré comme texte pour la géométrie
    StructField("geo_point_2d", StringType(), True),  # Considéré comme texte pour le point géographique
])

# Consommez les messages et envoyez-les à Spark
for message in consumer:
    print(f"Message brut reçu : {message.value}")

    if message.value:
        try:
            data = message.value  # Données déjà désérialisées
            print(f"Données reçues : {data}")

            if 'results' in data and isinstance(data['results'], list):
                for item in data['results']:
                    if isinstance(item, dict):
                        # Remplacer 'NULL' par None et supprimer les clés avec des valeurs None
                        item = {k: v if v not in (None, 'NULL') else None for k, v in item.items()}
                        
                        # Convertir les données en DataFrame Spark
                        try:
                            df = spark.createDataFrame([item], schema=schema)
                            df.show(truncate=False)
                        except Exception as e:
                            print(f"Erreur lors de la création du DataFrame : {e}")
                    else:
                        print(f"L'élément n'est pas un dictionnaire : {item}")
            else:
                print("Le dictionnaire ne contient pas de liste sous la clé 'results'.")

        except Exception as e:
            print(f"Erreur lors du traitement des données : {e}")
    else:
        print("Message vide reçu, ignoré.")

# Fermez la session Spark à la fin (en théorie, ce ne sera jamais atteint ici)
spark.stop()
