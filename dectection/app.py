from flask import Flask, request, jsonify
from yolo import dectect_elements
import pika  # RabbitMQ
from minio import Minio
import cv2
import numpy as np
from color import get_color_name
from flask_sqlalchemy import SQLAlchemy



app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://username:password@postgres:5432/main"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


db = SQLAlchemy(app)


# TODO: This credentials is not persistent,needs to be generated multiple times
minio_client = Minio("127.0.0.1:9000",
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

def closest_color(rgb_tuple):
    # Define a list of specific colors
    specific_colors = {
        'red': (255, 0, 0),
        'green': (0, 255, 0),
        'blue': (0, 0, 255),
        'yellow': (255, 255, 0),
        'cyan': (0, 255, 255),
        'magenta': (255, 0, 255),
        'white': (255, 255, 255),
        'black': (0, 0, 0),
        'grey': (169, 169, 169),
        # Add more specific colors as needed
    }
    
    min_distance = float('inf')
    closest_color_name = None
    
    # Iterate over specific colors to find the closest match
    for color_name, color_rgb in specific_colors.items():
        distance = sum((a - b) ** 2 for a, b in zip(rgb_tuple, color_rgb))
        if distance < min_distance:
            min_distance = distance
            closest_color_name = color_name
    
    return closest_color_name

def get_avg_color(image, bounding_boxes):
    #print(image)
    #print(type(image))
    #image = cv2.imread(image)
    
    # Convert BGR to RGB
    image_rgb = cv2.cvtColor(image, cv2.COLOR_BGR2RGB)
    
    # Iterate through each bounding box
    for box in bounding_boxes:
        x1, y1, x2, y2 = box['x1'], box['y1'], box['x2'], box['y2']
        
        # Extract the region of interest (ROI)
        roi = image_rgb[y1:y2, x1:x2]
        
        # Calculate the average color of the ROI
        avg_color_per_row = np.average(roi, axis=0)
        avg_color = np.average(avg_color_per_row, axis=0)
        
        # Convert the average color to RGB
        avg_color_rgb = tuple(map(int, avg_color))
        
        # Get the closest color name
        color_name = closest_color(avg_color_rgb)
        
        # Print the detected color name
        return color_name

def crop_image_for_color(image, bbox):
    x1, y1, x2, y2 = bbox['x1'], bbox['y1'], bbox['x2'], bbox['y2']
    # Crop the image based on the bounding box
    cropped_image = get_color_name(image[y1:y2, x1:x2])
    return True
  

def frames_color_to_db(yolo_resp, color):
    dectection_entry_db = 
    return True


def process_message(ch, method, properties, body):
    print(f' [x] Received {body}')
    image_obj = fetch_from_minio(body)  
    image_data = image_obj.read()
    image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)  
    yolo_response = dectect_elements(body, image)
    color = get_avg_color(image, yolo_response)
    frames_color_to_db(yolo_resp=yolo_response, color=color)



# TODO : Create a queue 'raw-frames' before hand, so this won't fail
def connect_and_listen():
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='127.0.0.1'))
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

