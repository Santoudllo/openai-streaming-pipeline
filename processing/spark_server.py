from flask import Flask, request, jsonify
from pyspark.sql import SparkSession

app = Flask(__name__)
spark = SparkSession.builder.appName("Spark Server").getOrCreate()

@app.route('/')
def index():
    return "Welcome to the Spark Server!"

@app.route('/process_data', methods=['POST'])
def process_data():
    json_data = request.json
    # Traiter les données ici, par exemple en les convertissant en DataFrame
    df = spark.read.json(sc.parallelize([json_data]))  # Exemple de création d'un DataFrame

    # Afficher le DataFrame pour voir les données
    df.show()

    return jsonify({
        "status": "success",
        "message": "Data processed successfully"
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)  # Exposer le serveur sur toutes les interfaces
