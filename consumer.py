from kafka import KafkaConsumer
from json import loads
import joblib
import pandas as pd

import db_operations as db

model = joblib.load('rf_regressor.pkl')

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
   # print(pred_df.head())
   prediction = model.predict(pred_df)

   df['Predicted Happiness Score'] = prediction

   # cols = ['GDP per capita', 'Health (Life Expectancy)', 'Freedom', 'Generosity', 'Government Corruption', 'Happiness Score', 'Predicted Happiness Score']
   # df[cols] = df[cols].astype(float)
   print(df.head())

   return df

def kafka_consumer():
    
    consumer = KafkaConsumer(
        'test-data',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
        )

    for m in consumer:
      row = predict(m)
      db.load(row)

if __name__ == '__main__':
   db.create_table()
   kafka_consumer()