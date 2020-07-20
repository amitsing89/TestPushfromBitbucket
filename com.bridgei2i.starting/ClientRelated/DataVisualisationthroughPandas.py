import re
import matplotlib.pyplot as plt
import pandas as pd
from kafka import KafkaConsumer
import json

from textblob import TextBlob

plt.ion()
fig, ax = plt.subplots()


def clean_tweet(tweet):
    '''
    Utility function to clean tweet text by removing links, special characters
    using simple regex statements.
    '''
    return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t ])|(\w+:\/\/\S+)", " ", tweet).split())


def get_tweet_sentiment(tweet):
    '''
    Utility function to classify sentiment of passed tweet
    using textblob's sentiment method
    '''
    # create TextBlob object of passed tweet text
    analysis = TextBlob(clean_tweet(tweet))
    # set sentiment
    if analysis.sentiment.polarity > 0:
        return 'positive'
    elif analysis.sentiment.polarity == 0:
        return 'neutral'
    else:
        return 'negative'


if __name__ == '__main__':
    parsed_topic_name = 'twitter-topic-sentiments'
    df = pd.DataFrame()
    consumer = KafkaConsumer(parsed_topic_name, bootstrap_servers=['localhost:9092'],
                             auto_offset_reset='latest', enable_auto_commit=True,
                             auto_commit_interval_ms=1000, group_id=None)
    for msg in consumer:
        record = json.loads(msg.value)
        record['sentiment'] = get_tweet_sentiment(record['text'])
        df = df.append({'sentiments': record['sentiment']}, ignore_index=True)
        plot_df = df['sentiments'].value_counts()
        if len(plot_df) >= 3:
            y = [plot_df[1], plot_df[2], plot_df[0]]
            print(y)
            plt.pause(0.0001)
            ax.plot(['Positive', 'Negative', 'Neutral'], [plot_df[1], plot_df[2], plot_df[0]], ['Positive'], plot_df[1],
                    'r^', 'Negative', plot_df[2], 'bs', 'Neutral', plot_df[0], 'g^')
