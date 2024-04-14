from flask import Flask, request, jsonify
from yolo import dectect_elements
import pika  # RabbitMQ
from minio import Minio
import cv2
import numpy as np
# from color import get_color_name
import hashlib
import datetime
from flask_sqlalchemy import SQLAlchemy


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = "postgresql://username:password@127.0.0.1:5432/main"
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False


db = SQLAlchemy(app)

class Frames(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    video_id = db.Column(db.Integer, db.ForeignKey('videos.id'), nullable=False)
    frame_path = db.Column(db.String(1000), nullable=False) # Minio Path
    processed = db.Column(db.Boolean, default=False)
    text = db.Column(db.String(10000), nullable=True)

    def __repr__(self):
        return '<Video %r>' % self.id
    
class Dectections(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    frame_id = db.Column(db.Integer, db.ForeignKey('frames.id'), nullable=False)
    x1 = db.Column(db.Integer, nullable=False)
    y1 = db.Column(db.Integer, nullable=False)
    x2 = db.Column(db.Integer, nullable=False)
    y2 = db.Column(db.Integer, nullable=False)
    obj = db.Column(db.String(100), nullable=False)
    color = db.Column(db.String(100), nullable=False)
    label = db.Column(db.String(1000), nullable=False)
    ocr = db.Column(db.String(10000), nullable=True)
    video_name  = db.Column(db.String(1000), nullable=False)
    snapshot_time = db.Column(db.DateTime, nullable=False)

    def __repr__(self):
        return '<Detection %r>' % self.id

# TODO: This credentials is not persistent,needs to be generated multiple times
minio_client = Minio("127.0.0.1:9000",
                     access_key="osUFvMkVb9mkNApv",
                     secret_key="YTxEZEoh9DeCYrT70xVexJn6PZgoB5i3",
                     secure=False)

# TODO : Move this to a separate file
def Calculate_hash(file):
    return hashlib.md5(file.read()).hexdigest()


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
  

def get_frame_id(frame_path):
    with app.app_context():
        frame_path =  frame_path.decode('utf-8')
        query = db.session.query(Frames.id).filter(Frames.frame_path == frame_path).order_by(Frames.id.desc()).limit(1)
        last_inserted_id = query.first()
        
        if last_inserted_id:
            return last_inserted_id[0]
        else:
            return 0

def frames_color_to_db(frame_path, yolo_resp, color):
    with app.app_context():
        for box in yolo_resp:

            fetch_frame_id = get_frame_id(frame_path)
            x1, y1, x2, y2 = box['x1'], box['y1'], box['x2'], box['y2']
            x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
            video_name, time = box['id'].split('/')[1:3]
            dectected_obj = box['class']
            color = color
            label = box['id']
            timestamp_str = time.split('.')[0]
            print(timestamp_str)
            dt = datetime.datetime.strptime(timestamp_str, '%Y-%m-%d-%H-%M-%S')
            time_postgres_format = dt.strftime("%Y-%m-%d %H:%M:%S")
            ocr =""
            fetch_frame_id = get_frame_id(frame_path)
            dectection_entry_obj = Dectections(frame_id=fetch_frame_id,
                                               x1=x1, 
                                               y1=y1,
                                               x2=x2,
                                                y2=y2,
                                                obj=dectected_obj,
                                                color=color,
                                                label=label,
                                                snapshot_time=time_postgres_format,
                                                video_name=video_name,
                                                ocr=ocr )
            db.session.add(dectection_entry_obj)
            db.session.commit()
            print("Dectection Entry Added   ")
    return True


def process_message(ch, method, properties, body):
    print(f' [x] Received {body}')
    image_obj = fetch_from_minio(body)  
    image_data = image_obj.read()
    image = cv2.imdecode(np.frombuffer(image_data, np.uint8), cv2.IMREAD_COLOR)  
    yolo_response = dectect_elements(body, image)
    color = get_avg_color(image, yolo_response)
    frames_color_to_db(frame_path=body, yolo_resp=yolo_response, color=color)



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

