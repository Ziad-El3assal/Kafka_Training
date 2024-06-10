from ultralytics import YOLO
from PIL import Image
model = YOLO("yolov8n.pt")

def predict(image):
    ret=[]
    results = model(image)
    for result in results:
        for pred in result.boxes.data:
            conf=pred[4]
            if conf<0.7:
                continue
            label = model.names[int(pred[5])]  # Get the class label
            ret.append(label)
    if ret==[]:
        ret.append(result.labels[0])
    return ret[0]
