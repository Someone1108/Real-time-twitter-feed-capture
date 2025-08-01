import streamlit as st
from kafka import KafkaConsumer
import json
import matplotlib.pyplot as plt
import pandas as pd
import datetime
from collections import Counter
import re
import uuid

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

# Initialize session state
if 'sentiment_count' not in st.session_state:
    st.session_state.sentiment_count = {'positive': 0, 'negative': 0, 'neutral': 0}
    st.session_state.search_sentiment_count = {'positive': 0, 'negative': 0, 'neutral': 0}
    st.session_state.search_match_total = 0
    st.session_state.match_log = []
    st.session_state.match_trend = []
    st.session_state.word_counter = Counter()

# Search input
search_term = st.text_input(" Enter a word to track in tweets:", value="sports")

# Reset button
if st.button(" Reset Search Stats"):
    st.session_state.search_sentiment_count = {'positive': 0, 'negative': 0, 'neutral': 0}
    st.session_state.search_match_total = 0
    st.session_state.match_log = []
    st.session_state.match_trend = []

# Placeholders
chart_placeholder = st.empty()
tweet_placeholder = st.empty()
line_chart_placeholder = st.empty()
stats_placeholder = st.empty()
suggest_placeholder = st.empty()

# Stream processing loop
for message in consumer:
    tweet_data = message.value
    text = tweet_data.get("text", "").lower()
    sentiment = tweet_data.get("sentiment", "neutral")
    timestamp = tweet_data.get("created_at", str(datetime.datetime.now()))
    username = tweet_data.get("username", "Unknown")
    hashtags = tweet_data.get("hashtags", [])

    # Update sentiment count
    st.session_state.sentiment_count[sentiment] += 1

    # Update common word counter
    words = re.findall(r'\b\w+\b', text)
    st.session_state.word_counter.update(words)

    # Search match tracking
    if search_term.lower() in text:
        st.session_state.search_match_total += 1
        st.session_state.search_sentiment_count[sentiment] += 1
        st.session_state.match_log.append({
            "timestamp": timestamp,
            "text": text,
            "sentiment": sentiment,
            "user": username
        })
        st.session_state.match_trend.append({
            "time": datetime.datetime.now().strftime("%H:%M:%S"),
            "count": st.session_state.search_match_total
        })

    # Pie chart
    labels = list(st.session_state.sentiment_count.keys())
    values = list(st.session_state.sentiment_count.values())
    fig, ax = plt.subplots(figsize=(4.5, 4.5), dpi=100)
    ax.pie(values, labels=labels, autopct='%1.1f%%')
    ax.axis('equal')
    fig.tight_layout(pad=1.0)

    with chart_placeholder.container():
        col1, col2, col3 = st.columns([1, 2, 1])
        with col2:
            st.pyplot(fig)
    plt.close(fig)

    # Latest tweet display
    with tweet_placeholder.container():
        st.subheader(" Latest Tweet Feed")
        st.markdown(f"**Text:** {tweet_data.get('text', 'N/A')}")
        st.markdown(f"**Time:** {timestamp}")
        st.markdown(f"**Sentiment:** :{'green' if sentiment == 'positive' else 'red' if sentiment == 'negative' else 'gray'}[{sentiment}]")
        st.markdown(f"**User:** @{username}")
        if hashtags:
            st.markdown(f"** Hashtags:** {' '.join(['#' + tag for tag in hashtags])}")

    # Trend line chart
    if st.session_state.match_trend:
        trend_df = pd.DataFrame(st.session_state.match_trend)
        trend_df = trend_df.groupby("time").max().reset_index()
        with line_chart_placeholder.container():
            st.subheader("Mentions Over Time")
            st.line_chart(trend_df.set_index("time"))

    # Sentiment breakdown for keyword
    with stats_placeholder.container():
        st.subheader(f" Tracking matches for: :green[{search_term}]")
        st.markdown(f"- **Total Matches:** {st.session_state.search_match_total}")
        st.markdown(f"- **Positive:** {st.session_state.search_sentiment_count['positive']}")
        st.markdown(f"- **Negative:** {st.session_state.search_sentiment_count['negative']}")
        st.markdown(f"- **Neutral:** {st.session_state.search_sentiment_count['neutral']}")

        # Export to CSV with dynamic key to avoid duplication
        if st.session_state.match_log:
            export_df = pd.DataFrame(st.session_state.match_log)
            csv = export_df.to_csv(index=False).encode('utf-8')
            export_key = f"export_{uuid.uuid4()}"  # generate per rerun
            st.download_button(" Export Matched Tweets to CSV", data=csv, file_name="matched_tweets.csv", mime='text/csv', key=export_key)

    # Keyword suggestions
    with suggest_placeholder.container():
        st.subheader(" Suggested Keywords (most common words so far):")
        top_words = [w for w, _ in st.session_state.word_counter.most_common(10) if len(w) > 3]
        st.markdown(" | ".join(f"`{word}`" for word in top_words))

    #break  # remove this for continuous streaming
