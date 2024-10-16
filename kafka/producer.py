from kafka import KafkaProducer
import json
import requests
import os
from dotenv import load_dotenv

# Chargez les variables d'environnement à partir du fichier .env
load_dotenv()

# Récupérez l'URL de l'API depuis la variable d'environnement
api_url = os.getenv('API_URL')

# Configurez votre producteur Kafka
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

# Récupérer des données de l'API
try:
    response = requests.get(api_url)
    response.raise_for_status()  # Vérifie si la requête a réussi

    data = response.json()  # Tente de désérialiser la réponse en JSON

    # Imprimer les données pour vérifier leur format
    print(f"Envoi de données : {data}")

    # Envoyer le message au topic circulation
    producer.send('circulation', value=data)

    # Assurez-vous que les messages sont envoyés
    producer.flush()
    print("Message envoyé au topic circulation.")

except requests.exceptions.RequestException as e:
    print(f"Erreur lors de la récupération des données : {e}")
except json.JSONDecodeError:
    print("Les données récupérées ne sont pas au format JSON.")
