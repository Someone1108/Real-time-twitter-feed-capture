from kafka import KafkaConsumer, KafkaProducer
from textblob import TextBlob
import json

# Kafka setup
consumer = KafkaConsumer(
    'raw_tweets',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',  #  This already exists
    group_id=None  #  ADD THIS
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print(" Listening for tweets...")

for message in consumer:
    tweet = message.value
    text = tweet.get('text', '')

    # Sentiment Analysis
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    sentiment = (
        'positive' if polarity > 0.1 else
        'negative' if polarity < -0.1 else
        'neutral'
    )

    tweet['sentiment'] = sentiment
    tweet['polarity'] = polarity
    tweet['username'] = tweet.pop('user', 'Unknown') 
    # Send to processed_tweets
    producer.send('processed_tweets', tweet)

    print(f" Processed: ({sentiment}) {text[:60]}...")