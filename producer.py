from kafka import KafkaProducer
#from confluent_kafka import Producer
import json
import pandas as pd
from time import sleep
import datetime as dt

def kafka_producer():
    producer = KafkaProducer(
        value_serializer = lambda m: json.dumps(m).encode('utf-8'),
        bootstrap_servers = ['localhost:9092'],
    )

    test_df = pd.read_csv("./data/test.csv")

    # for chunk in df:
    #     data = chunk.to_json() # turn data into json
    #     # print(type(data))
    #     producer.send("kafka_lab2", value=data) # sed data to topic kafka_lab2
    #     sleep(5)

    #     print(f"new chunk sent at {dt.datetime.utcnow()}")

    for i in range(len(test_df)):
        row_json = test_df.iloc[i].to_json()
        producer.send("test_data", value=row_json)

        print(f"message sent at {dt.datetime.utcnow()}")
        sleep(2)


kafka_producer()
