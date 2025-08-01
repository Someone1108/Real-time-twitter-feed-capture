import streamlit as st
from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt

# Streamlit setup
st.set_page_config(page_title="Twitter Sentiment", layout="wide")
st.title("Real-Time Twitter Sentiment Dashboard")

# Kafka Consumer setup
consumer = KafkaConsumer(
    'processed_tweets',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    group_id=None
)

# Sentiment counter
sentiment_count = {'positive': 0, 'negative': 0, 'neutral': 0}

# Search term setup
search_term = st.text_input(" Enter a word to track in tweets:", value="sports")
search_match_total = 0
search_sentiment_count = {'positive': 0, 'negative': 0, 'neutral': 0}

# Placeholders
chart_placeholder = st.empty()
search_placeholder = st.empty()
tweet_placeholder = st.empty()

# Stream processing loop
for message in consumer:
    tweet_data = message.value
    text = tweet_data.get("text", "").lower()
    sentiment = tweet_data.get("sentiment", "neutral")
    timestamp = tweet_data.get("created_at", "N/A")
    username = tweet_data.get("username", "Unknown")
    hashtags = tweet_data.get("hashtags", [])

    sentiment_count[sentiment] += 1

    # Search term logic
    if search_term.lower() in text:
        search_match_total += 1
        search_sentiment_count[sentiment] += 1

    # Pie chart update
    labels = list(sentiment_count.keys())
    values = list(sentiment_count.values())
    fig, ax = plt.subplots(figsize=(3.5, 3.5), dpi=100)
    ax.pie(values, labels=labels, autopct='%1.1f%%')
    ax.axis('equal')
    fig.tight_layout(pad=1.0)

    with chart_placeholder.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.pyplot(fig)
    plt.close(fig)

    # Tweet feed update
    with tweet_placeholder.container():
        st.subheader(" Latest Tweet Feed")
        st.markdown(f"**Text:** {tweet_data.get('text', 'N/A')}")
        st.markdown(f"**Time:** {timestamp}")
        st.markdown(f"**Sentiment:** :{'green' if sentiment == 'positive' else 'red' if sentiment == 'negative' else 'gray'}[{sentiment}]")
        st.markdown(f"**User:** @{username}")
        if hashtags:
            st.markdown(f"** Hashtags:** {' '.join(['#' + tag for tag in hashtags])}")

    # Search match update
    with search_placeholder.container():
        st.subheader(f" Tracking matches for: `{search_term}`")
        st.markdown(f"- **Total Matches:** {search_match_total}")
        st.markdown(f"- **Positive:** {search_sentiment_count['positive']}")
        st.markdown(f"- **Negative:** {search_sentiment_count['negative']}")
        st.markdown(f"- **Neutral:** {search_sentiment_count['neutral']}")