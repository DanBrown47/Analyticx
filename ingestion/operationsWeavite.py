import weaviate
import os
import base64

client = weaviate.Client('http://172.17.0.1:8080')

def convert_2_base64(image_path) -> str:
    try:
        with open(image_path, "rb") as image_file:
            image_data = image_file.read()
    except FileNotFoundError:
        raise FileNotFoundError(f"Image file not found: {image_path}")

    # Encode image data as base64 string
    base64_encoded = base64.b64encode(image_data).decode("utf-8")
    return base64_encoded

def insert_to_weavite(image_path):
    encoded_string = convert_2_base64(image_path)

    data_object = {
        "image": encoded_string,  
        "filepath": image_path 
    }
    with client.batch() as batch:
        batch.add_data_object(data_object, "X")

   