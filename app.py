
import json
import time
import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt

from confluent_kafka import Consumer, KafkaError

st.title('Real-time Tweets Sentiment Analysis')

bootstrap_servers = 'localhost:9095'
topic = 'ml_result'
group_id = 'streamlit_consumer_group'

consumer = Consumer({
    'bootstrap.servers': bootstrap_servers,
    'group.id': group_id,
    'auto.offset.reset': 'earliest'
})
# st.write(f"Subscribed to topic: {topic}") 
consumer.subscribe([topic])


def get_new_messages(consumer):
    messages = []
    msg = consumer.poll(1.0)
    if msg is None:
        pass
    elif msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            st.write('End of partition reached')
        else:
            st.write(f'error: {msg.error()}')
    else:
        result = json.loads(msg.value().decode('utf-8'))
        messages.append(result)
    return messages

def main():
    n = 10
    st.write(f"Table of recent {n} tweets:")

    table_container = st.empty()
    chart_container = st.empty()

    sentiment_counts = {'positive': 0, 'negative': 0}

    tweet_data = []
    initial_data = {'Tweet Text': [''] * n, 'Predicted Class': [''] * n, 'Confidence': [''] * n}
    df_tweets = pd.DataFrame(initial_data)
    table_container.table(df_tweets)

    while True:
        messages = get_new_messages(consumer)
        if messages:
            for result in messages:
                sentiment = result['sentiment'].lower() 
                confidence = result.get('confidence', 0.0)

                if sentiment in sentiment_counts:
                    sentiment_counts[sentiment] += 1

                tweet_data.insert(0, {
                    'Tweet Text': result['tweet'],
                    'Predicted Class': result['sentiment'],
                    'Confidence': f"{confidence:.4f}"
                })

                if len(tweet_data) > n:
                    tweet_data.pop()

                for i in range(min(n, len(tweet_data))):
                    df_tweets.at[i, 'Tweet Text'] = tweet_data[i]['Tweet Text']
                    df_tweets.at[i, 'Predicted Class'] = tweet_data[i]['Predicted Class']
                    df_tweets.at[i, 'Confidence'] = tweet_data[i]['Confidence']
                table_container.table(df_tweets)

        df_sentiment = pd.DataFrame({
            'Sentiment': list(sentiment_counts.keys()),
            'Number of Tweets': list(sentiment_counts.values())
        })

        fig, ax = plt.subplots()
        ax.bar(df_sentiment['Sentiment'], df_sentiment['Number of Tweets'], color=['green', 'red'])
        ax.set_xlabel('Sentiment')
        ax.set_ylabel('Number of Tweets')
        ax.set_title('Distribution of Tweet Sentiments')
        chart_container.pyplot(fig)
        time.sleep(1)

if __name__ == "__main__":
    main()
     