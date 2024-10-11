import socket
import threading
import json

def handle_client(client_socket):
    while True:
        data = client_socket.recv(1024).decode()
        if not data:
            print("Connexion fermée par le client.")
            break
        try:
            # décoder les données JSON reçues

            json_data = json.loads(data)
            print(f"Données reçues : {json_data}")  # les données reçues
        except json.JSONDecodeError:
            print("Erreur de décodage JSON : ", data)  # les erreurs de décodage

    client_socket.close()

server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind(('localhost', 1234))  
server_socket.listen(5)
print("En attente d'une connexion...")

while True:
    client_socket, address = server_socket.accept()
    print(f"Connexion établie avec {address}")
    client_handler = threading.Thread(target=handle_client, args=(client_socket,))
    client_handler.start()
