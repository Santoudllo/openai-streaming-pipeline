import socket
import json
import requests
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv()

API_URL = os.getenv("API_URL")

# Obtenir les données de l'API
response = requests.get(API_URL)
data = response.json()

print(f"Contenu de la réponse API : {data}")

# Créer une connexion socket au serveur
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect(('localhost', 12345))

# Envoyer les données JSON au serveur
try:
    json_data = json.dumps(data)  # Convertir les données en JSON
    client_socket.sendall(json_data.encode('utf-8'))  # Envoyer les données

    # Attendre une réponse du serveur (facultatif)
    server_response = client_socket.recv(1024).decode('utf-8')
    print(f"Réponse du serveur : {server_response}")
finally:
    # Fermer la connexion après avoir envoyé les données
    client_socket.close()
