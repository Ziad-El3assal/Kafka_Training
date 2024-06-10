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



me = "Refresher"
Conf={
    "bootstrap.servers":"34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094",
    "group.id":"mygroup",
    "auto.offset.reset":"latest",
}

Consumer=Consumer(Conf)
topics=[me]
IMAGES_DIR = "images"


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
                
                requests.put('http://127.0.0.1:5000/refresher/')
    finally:
        consumer.close()
        
def shutdown():
    print("Shutting down") 
    Consumer.close()
    exit(0) 
    
while running:
    consume()
    shutdown()