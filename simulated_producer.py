import pandas as pd
import time
import json
from kafka import KafkaProducer

# Load dataset
df = pd.read_csv("data/tweets.csv", encoding='latin-1', header=None)
df.columns = ['sentiment', 'id', 'date', 'query', 'user', 'text']

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Simulate tweet stream
for _, row in df.iterrows():
    tweet = {
        'id': int(row['id']),
        'user': row['user'],
        'text': row['text'],
        'created_at': row['date']
    }
    producer.send('raw_tweets', tweet)
    print(f" Sent tweet by {tweet['user']}: {tweet['text'][:60]}...")
    time.sleep(1)  # Simulate 1 tweet per second

print("Finished streaming all tweets.")