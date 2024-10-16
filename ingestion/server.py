import socket
import threading
import json

def handle_client(client_socket):
    buffer_size = 4096
    data_buffer = ""

    while True:
        try:
            # Réception des données
            data = client_socket.recv(buffer_size).decode()
            if not data:
                print("Connexion fermée par le client.")
                break

            data_buffer += data

            # Essayer de décoder les données JSON une fois complètes
            try:
                # Tenter de charger les données JSON
                json_data = json.loads(data_buffer)
                print(f"Données reçues : {json_data}")

                # Réinitialiser le tampon après traitement
                data_buffer = ""

                # Envoyer une réponse au client
                client_socket.sendall("Données reçues avec succès.".encode('utf-8'))

            except json.JSONDecodeError:
                # Si les données sont incomplètes, continuer à recevoir
                continue

        except Exception as e:
            print(f"Erreur lors du traitement des données : {e}")
            break

    client_socket.close()

# Configuration du serveur
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('localhost', 12345))  # Utilisez le même port que le client
server_socket.listen(5)
print("Serveur en attente d'une connexion...")

while True:
    client_socket, address = server_socket.accept()
    print(f"Connexion établie avec {address}")
    client_handler = threading.Thread(target=handle_client, args=(client_socket,))
    client_handler.start()
