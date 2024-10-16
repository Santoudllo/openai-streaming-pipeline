from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import from_json

# Créer une session Spark
spark = SparkSession.builder \
    .appName("Kafka Consumer") \
    .getOrCreate()

# Définir le schéma
schema = StructType([
    StructField("identifiant", StringType(), True),
    StructField("identifiant_ctv", StringType(), True),
    StructField("cp_arrondissement", StringType(), True),
    StructField("numero_stv", StringType(), True),
    StructField("typologie", StringType(), True),
    StructField("maitre_ouvrage", StringType(), True),
    StructField("objet", StringType(), True),
    StructField("description", StringType(), True),
    StructField("voie", StringType(), True),
    StructField("precision_localisation", StringType(), True),
    StructField("date_debut", StringType(), True),
    StructField("date_fin", StringType(), True),
    StructField("impact_circulation", StringType(), True),
    StructField("impact_circulation_detail", StringType(), True),
    StructField("niveau_perturbation", StringType(), True),
    StructField("statut", StringType(), True),
    StructField("geo_shape", StringType(), True),
    StructField("geo_point_2d", StringType(), True)
])

# Lire le stream Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "circulation") \
    .load()

# Convertir la colonne de valeur en DataFrame avec le schéma
json_df = df.selectExpr("CAST(value AS STRING) as json") \
    .select(from_json("json", schema).alias("data")) \
    .select("data.*")

# Afficher le stream
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
