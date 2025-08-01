import os
import json
import datetime
import pandas as pd
from kafka import KafkaConsumer

# Settings
TOPIC = 'processed_tweets'
BATCH_SIZE = 100
BASE_DIR = 'silver'  # Destination folder

# Kafka Consumer setup
consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='parquet_etl_group'
)

print(f"Listening to Kafka topic: {TOPIC}")

buffer = []

for message in consumer:
    tweet = message.value
    buffer.append(tweet)

    if len(buffer) >= BATCH_SIZE:
        # Determine today's date folder
        today = datetime.date.today().isoformat()
        output_dir = os.path.join(BASE_DIR, today)
        os.makedirs(output_dir, exist_ok=True)

        # File name with timestamp
        timestamp = datetime.datetime.now().strftime("%H%M%S")
        file_path = os.path.join(output_dir, f"tweets_batch_{timestamp}.parquet")

        # Save to Parquet
        df = pd.DataFrame(buffer)
        df.to_parquet(file_path, index=False, engine='pyarrow')

        print(f"Saved {len(buffer)} tweets to {file_path}")
        buffer = []  # Clear buffer
