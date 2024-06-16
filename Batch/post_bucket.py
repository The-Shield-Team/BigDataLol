import functions_framework
import os
import json
import requests
from google.cloud import storage
from flask import Flask, request


# Obtener las variables de entorno
project_id = os.getenv('PROJECT')
bucket_name = os.getenv('BUCKET')
file_name = 'champs/champs.json'
url = os.getenv('URL')
to = os.getenv('TO')

def send_mail(subject, body):
    payload = {
        "to": to,
        "subject": subject,
        "body": body,
    }
    headers = {'Content-Type': 'application/json'}
    try:
        response = requests.post(url, data=json.dumps(payload), headers=headers)
        if response.status_code == 200:
            print("Correo electrónico enviado exitosamente")
        else:
            print("Error al enviar el correo electrónico. Código de estado:", response.status_code)
            print("Mensaje de error:", response.text)
    except Exception as e:
        print("Error:", e)

@functions_framework.http
def post_bucket_lol_champs(request):
    """HTTP Cloud Function.
    Args:
        request (flask.Request): The request object.
        <https://flask.palletsprojects.com/en/1.1.x/api/#incoming-request-data>
    Returns:
        The response text, or any set of values that can be turned into a
        Response object using `make_response`
        <https://flask.palletsprojects.com/en/1.1.x/api/#flask.make_response>.
    """
    try:
        if not request.is_json:
            send_mail("Error al subir datos al bucket", "El contenido de la solicitud no es JSON")
            return json.dumps({'error': 'Request content is not JSON'}), 400

        data = request.get_json()

        # Verificar si los datos están presentes en el JSON
        if 'data' not in data:
            send_mail("Error al subir datos al bucket", "La llave 'data' no está en el JSON")
            return json.dumps({'error': "'data' key not found in JSON"}), 400

        # Obtener los datos del JSON
        json_data = data['data']
        
        # Convertir los datos JSON a un string
        json_str = json.dumps(json_data)

        # Iniciar el cliente de almacenamiento
        storage_client = storage.Client(project=project_id)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(file_name)

        # Subir el contenido del JSON al bucket como un archivo .json
        blob.upload_from_string(json_str, content_type='application/json')
        send_mail("Datos subidos al bucket", "Los datos JSON han sido subidos con éxito")
        return json.dumps({'message': 'JSON data uploaded successfully'}), 200
    except Exception as e:
        # Manejo de excepciones y registro del error
        send_mail("Error inesperado", str(e))
        return json.dumps({'error': str(e)}), 500


if __name__ == '__main__':
    app = Flask(__name__)

    @app.route('/post_to_bucket', methods=['POST'])
    def post_bucket_lol_champs_route():
        return post_bucket_lol_champs(request)

    # Escuchar en el puerto proporcionado por la variable de entorno PORT o 8080 por defecto
    port = int(os.getenv('PORT', 8080))
    app.run(host='0.0.0.0', port=port)



'''
functions-framework==3.*
Flask==2.*
gunicorn==20.*

''''''''
functions-framework==3.*
google-cloud-storage==2.3.0
flask == 3.0.2
werkzeug ==3.*

'''

