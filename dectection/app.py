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


import cv2

def get_average_color(image, bbox):
  """
  This function takes an image and a bounding box (as a dictionary with x1, y1, x2, y2 coordinates)
  and returns the average color within that bounding box.

  Args:
      image: The image as a NumPy array.
      bbox: A dictionary containing bounding box coordinates (x1, y1, x2, y2).

  Returns:
      A list containing the average BGR values of the object in the bounding box.
  """
  x1, y1, x2, y2 = bbox['x1'], bbox['y1'], bbox['x2'], bbox['y2']
  # Crop the image based on the bounding box
  cropped_image = image[y1:y2, x1:x2]
  # Calculate the average color across all channels (BGR)
  average_color = cv2.mean(cropped_image)
  return average_color

def process_yolo_response(image, yolo_response):
  colors = []
  for detection in yolo_response:
    bbox = detection
    avg_color = get_average_color(image.copy(), bbox)
    colors.append([avg_color[0], avg_color[1], avg_color[2], detection['class']])
  return colors

  

def frames_color_to_db(response):
    print(response)
    return True

# Implement this here https://medium.com/codex/rgb-to-color-names-in-python-the-robust-way-ec4a9d97a01f TODO #1 Priority

def process_message(ch, method, properties, body):
    print(f' [x] Received {body}')
    image_obj = fetch_from_minio(body)  
    image_data = image_obj.read()
    image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)  
    yolo_response = dectect_elements(body, image)
    print(yolo_response)

# TODO : Create a queue 'raw-frames' before hand, so this won't fail
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

# @app.route('/detect', methods=['POST']) # For testing purposes
# def detect():
#     image = request.files['image']
#     image_path = '/tmp/image.jpg'
#     image.save(image_path.data)
#     return dectect_elements(image_path)
