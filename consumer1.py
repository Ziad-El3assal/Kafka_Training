from confluent_kafka import Consumer
import sys
from confluent_kafka import KafkaError
me = "Ziad"
me1="Ziad1"
Conf={
    "bootstrap.servers":"34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094",
    "group.id":"mygroup",
    "auto.offset.reset":"smallest",
}

Consumer=Consumer(Conf)
topics=[me,me1]

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
            print('Received message: {} count {}'.format(msg.value().decode('utf-8'),cnt))
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