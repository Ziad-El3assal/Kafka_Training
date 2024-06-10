from confluent_kafka import Consumer
import sys
from confluent_kafka import KafkaError
import requests 
import random
import json
from prediction import predict
from PIL import ImageFont
from PIL import ImageDraw
from PIL import Image
from confluent_kafka import Producer


from inference_sdk import InferenceHTTPClient

CLIENT = InferenceHTTPClient(
    api_url="https://detect.roboflow.com",
    api_key="API_KEY"
)

def drawWatermark(image, text):
    watermark_image = image
 
    draw = ImageDraw.Draw(watermark_image)
    w, h = image.size
    x, y = int(w / 2), int(h / 2)
    if x > y:
        font_size = y
    elif y > x:
        font_size = x
    else:
        font_size = x
    
    font = ImageFont.truetype("arial.ttf", int(font_size))
    draw.text((x, y), text, fill=(0, 0, 0), font=font, anchor='ms')
    return watermark_image

me = "ZiadLab2"
Conf={
    "bootstrap.servers":"34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094",
    "group.id":"mygroup",
    "auto.offset.reset":"smallest",
}

Consumer=Consumer(Conf)
topics=[me]
IMAGES_DIR = "images"

def produceToDrawWatermark(path):
    topic="drawWatermark"
    nwconf={
    "bootstrap.servers":"34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094",
    "client.id":topic
}
    producer=Producer(nwconf)
    producer.produce(topic,key="asd",value=path)
    producer.flush()
    
def detect_object(image):
    result = predict(image)
    return result
def consume():
    basic_consume_loop(Consumer,topics)
    
running=True
def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)
        cnt=0
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                        (msg.topic(), msg.partition(), msg.offset()))
                else:
                    print(msg.error())
                    break
                
            msg=json.loads(msg.value())
            print(msg)
            #print('%% %s [%d] at offset %d with key %s:\n' %   (msg['filename'], msg['id'], msg['offset'], str(msg['key'])))    
            path="{}/{}".format(IMAGES_DIR, msg['filename'])
            id=msg['id']
            filename=msg['filename']
            img = Image.open("{}/{}".format(IMAGES_DIR, filename))
            detection=detect_object(path)
            print("Detection is done: ", detection)
            requests.put('http://127.0.0.1:5000/object/' + id, json={"object": detection})
            obj={"object": detection, "filename": path}
            produceToDrawWatermark(json.dumps(obj))
            cnt+=1
    finally:
        consumer.close()
        
def shutdown():
    print("Shutting down") 
    Consumer.close()
    exit(0) 
    
while running:
    consume()
    shutdown()