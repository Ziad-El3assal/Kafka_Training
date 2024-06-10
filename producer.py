from  confluent_kafka import Producer
import random
me = "Ziad"

conf={
    "bootstrap.servers":"34.68.55.43:9094,34.136.142.41:9094,34.170.19.136:9094",
    "client.id":me
}
i=0 
producer=Producer(conf)
while i<100:
    key = "".join([random.choice("abcdefghjklmnopqstrzxy") for i in range(10)])
    topic=me
    #msg=input("Enter message:")
    producer.produce(topic,key=key,value=str(i))
    producer.flush()
    print("Message sent")
    i+=1