import socket
import time
import json

# Créer un socket TCP
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Se connecter au serveur
client_socket.connect(('localhost', 1234))  # Le même port que dans server.py

# Simuler l'envoi de données
for i in range(10):
    data = {
        'id': i,
        'value': f'Donnée {i}',
        'timestamp': time.time()
    }
    client_socket.sendall(json.dumps(data).encode())
    print(f"Données envoyées : {data}")
    time.sleep(1)  # Attendre 1 seconde entre les envois

# Fermer la connexion
client_socket.close()
