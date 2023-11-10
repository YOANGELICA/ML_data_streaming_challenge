import datetime as dt
from json import dumps
from kafka import KafkaProducer
import pandas as pd
from time import sleep

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS
from utils.transformations import get_continent, del_cols, dummies


def get_test_data():

    test_df = pd.read_csv("data/x_test_data.csv")
    test_df['Continent'] = test_df['Country'].apply(get_continent)
    del_cols(test_df)
    test_df = dummies(test_df)

    y_test = pd.read_csv("data/y_test_data.csv")

    test_df['Happiness Score'] = y_test

    return test_df


def kafka_producer(test_df):
    producer = KafkaProducer(
        value_serializer = lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
    )

    for i in range(len(test_df)):
        row_json = test_df.iloc[i].to_json()
        producer.send("test-data", value=row_json)

        print(f"new message sent at {dt.datetime.utcnow()}")
        sleep(2)

    print("All rows sent")

if __name__ == '__main__':

    test_df = get_test_data()
    # print(test_df.columns)
    kafka_producer(test_df)
