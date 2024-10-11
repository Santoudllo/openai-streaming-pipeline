import socket
import threading

# Fonction pour gérer les connexions clients

def handle_client(client_socket):
    while True:
        # Recevoir des données du client

        data = client_socket.recv(1024).decode()
        if not data:
            break
        print(f"Données reçues : {data}")
    client_socket.close()

# Créer un socket TCP

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

# Lier le socket à l'adresse IP et au port

server_socket.bind(('localhost', 1234)) 

# Écouter les connexions entrantes

server_socket.listen(5)
print("En attente d'une connexion...")

# Accepter les connexions des clients

while True:
    client_socket, address = server_socket.accept()
    print(f"Connexion établie avec {address}")
    client_handler = threading.Thread(target=handle_client, args=(client_socket,))
    client_handler.start()
