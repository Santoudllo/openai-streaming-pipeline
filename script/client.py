import socket
import threading
import json

# Fonction pour gérer les connexions clients
def handle_client(client_socket):
    while True:
        # Recevoir des données du client
        data = client_socket.recv(1024).decode()
        if not data:
            break
        # Ici, vous pouvez traiter les données ou les envoyer à Apache Spark
        print(f"Données reçues : {data}")
        
        # Simuler l'envoi des données à un système de traitement (comme Apache Spark)
        # process_data(data)

    client_socket.close()

# Créer un socket TCP
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Lier le socket à l'adresse IP et au port
server_socket.bind(('localhost', 12345))

# Écouter les connexions entrantes
server_socket.listen(5)
print("En attente d'une connexion...")

# Accepter les connexions des clients
while True:
    client_socket, address = server_socket.accept()
    print(f"Connexion établie avec {address}")
    client_handler = threading.Thread(target=handle_client, args=(client_socket,))
    client_handler.start()
