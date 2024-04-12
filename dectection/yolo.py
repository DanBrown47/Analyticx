from flask import Flask
from ultralytics import YOLO
from PIL import Image
import json 
import numpy as np


model = YOLO('yolov8n.pt')
yolo_class_names = model.names


def np_encoder(object):
    if isinstance(object, np.generic):
        return object.item()
    

def dectect_elements(path, image):
    # Decode path from bytes to string
    path = path.decode('utf-8')

    # TODO Check for File quality [What if a URL]
    response = []
    results = model(image)  
    for result in results:
        boxes = result.boxes.cpu().numpy()
        for idx, box in enumerate(boxes): 
            box_data = {
                "id": str(path)+"_"+str(idx),
                "x1": box.xyxy[0][0].astype(int),
                "y1": box.xyxy[0][1].astype(int),
                "x2": box.xyxy[0][2].astype(int),
                "y2": box.xyxy[0][3].astype(int),
                "class": yolo_class_names[box.data[0][5].astype(int)]
            }

            response.append(box_data)

    # json_response = json.dumps(response, default=np_encoder) 
    return response


if __name__ == '__main__':
    image_path = '../frames/frame_19700101053001.jpg' # Temporarily hard-coded
    # image_path = '/tmp/pexels-markus-spiske-112784.jpg'
    print(dectect_elements(image_path))