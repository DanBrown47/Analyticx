from flask import Flask, request, jsonify
from yolo import dectect_elements
import pika  # RabbitMQ
from minio import Minio
import cv2
import numpy as np

app = Flask(__name__)

# TODO: This credentials is not persistent,needs to be generated multiple times
minio_client = Minio("minio:9000",
                     access_key="osUFvMkVb9mkNApv",
                     secret_key="YTxEZEoh9DeCYrT70xVexJn6PZgoB5i3",
                     secure=False)

# TODO : Move this to a separate file
def fetch_from_minio(frame_path):
    bucket_name = "storageone"
    obj =  minio_client.get_object(
        bucket_name,
        frame_path)
    
    return obj


@app.route('/detect', methods=['POST']) # For testing purposes
def detect():
    image = request.files['image']
    image_path = '/tmp/image.jpg'
    image.save(image_path.data)
    return dectect_elements(image_path)


def process_message(ch, method, properties, body):
    print(f' [x] Received {body}')
    # TODO : Fetch from Minio and respond to dectect elements use f_get_object
    image_obj = fetch_from_minio(body)
    iamge_data = image_obj.read()
    image = cv2.imdecode(np.frombuffer(iamge_data, np.uint8), cv2.IMREAD_COLOR)  
    yolo_response = dectect_elements(image)

def connect_and_listen():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
    channel = connection.channel()
    # channel.queue_declare(queue=queue_name) Should be declared in the ingestion module 

    channel.basic_consume(queue='raw-frames', 
                          on_message_callback=process_message, # which process to perform
                          auto_ack=True)  # Acknowledge messages automatically

    try:
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        channel.stop_consuming()
        connection.close()



@app.route('/')
def index():
    return 'Dectection Module of YOLO'

@app.route('/health')
def health():
    return 'YOLO-Dectection Landscape is Healthy'

connect_and_listen()  # Consider using a Thread for this
app.run(debug=True, port=8000, host='0.0.0.0') 
