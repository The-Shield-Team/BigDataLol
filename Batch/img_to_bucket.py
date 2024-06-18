import os
import base64
import json
import requests

def encode_images_in_folder(folder_path):
    images = []
    for filename in os.listdir(folder_path):
        if filename.endswith('.png') or filename.endswith('.jpg'):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'rb') as image_file:
                image_bytes = image_file.read()
                image_base64 = base64.b64encode(image_bytes).decode('utf-8')
                nombre, extension = os.path.splitext(filename)
                images.append({
                    "nombre": nombre,
                    "image": image_base64,
                    "extension": extension
                })
    return images

folder_path = 'C:/bigData/img/item'
version = '14.12'
type = 'item'
images_data = encode_images_in_folder(folder_path)

data = {
    "version": version,
    "images": images_data,
    "type": type

}
print(data)
response = requests.post('https://gatewayapi-bigdata-lol-2lrqhq48.uk.gateway.dev/post_images', json=data)
print(response.status_code)
print(response.json())



## traer todas las imagenes de un bucket
