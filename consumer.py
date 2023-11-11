from json import loads
import joblib
from kafka import KafkaConsumer
import pandas as pd

from config.kafka_config import KAFKA_BOOTSTRAP_SERVERS
import utils.db_operations as db

model = joblib.load('notebooks/rf_regressor.pkl')

def predict(m):
   s = loads(m.value)
   # s = m.value
   # print(s)
   # print(type(s))

   # create df
   data = {
        key: [value] for key, value in s.items()
    }
   
   df = pd.DataFrame(data)

   pred_df = df.drop(columns=['Happiness Score'], axis = 1)
#    print(pred_df.columns)
   prediction = model.predict(pred_df)

   df['Predicted Happiness Score'] = prediction

   # print(df.head())

   return df


def kafka_consumer():
    
    consumer = KafkaConsumer(
        'test-data',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS
    )

    while True:
        message = consumer.poll(timeout_ms=5000)  # wait 5 seconds for messages

        if not message:
            break  # no more messagges, exit the loop
        
        for _, messages in message.items():
            
            for m in messages:
                row = predict(m)
                db.load(row)

if __name__ == '__main__':
   
   db.create_table()
   kafka_consumer()