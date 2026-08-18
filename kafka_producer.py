from kafka import KafkaProducer
import json
import time
import pandas as pd

# Load a sample Reddit dataset
df = pd.read_csv('data/reddit_database.csv') 

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

for i, row in df.iterrows():
    data = {"text": str(row.get("post", ""))}
    producer.send('reddit_topic', value=data)
    print(f"Sent: {data}")
    time.sleep(1)