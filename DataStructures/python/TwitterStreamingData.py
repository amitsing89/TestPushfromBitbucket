import re
import tweepy
import json
from textblob import TextBlob
import Kafka_Producer
import redis

access_token_secret = 'pH3iIS4SNosfNYv9XVKmwSsLmT0hLjPlInO1iYS9f33Lu'
consumer_key = 'PzN3rCjnxaD7bbkKh7hUa8jb5'
consumer_secret = 'kepRSnVC7XzNbjs26DgVBczsWrJDCoLO2ascB8746zl3Ewxvp6'
access_token = '286178494-qdWjmeosEfVEbSHAVB3iJmwBicvgYiOIs4Xf0vOC'

filter_Words_Lists = ['#Metoo', '#metoo']

red = redis.Redis(host='localhost')


class TwitterHandler(tweepy.StreamListener):
    def clean_tweet(self, tweet):
        '''
        Utility function to clean tweet text by removing links, special characters
        using simple regex statements.
        '''
        return ' '.join(re.sub("(@[A-Za-z0-9]+)|([^0-9A-Za-z \t])|(\w+:\/\/\S+)", " ", tweet).split())

    def get_tweet_sentiment(self, tweet):
        '''
        Utility function to classify sentiment of passed tweet
        using textblob's sentiment method
        '''
        # create TextBlob object of passed tweet text
        analysis = TextBlob(self.clean_tweet(tweet))
        # set sentiment
        if analysis.sentiment.polarity > 0:
            return 'positive'
        elif analysis.sentiment.polarity == 0:
            return 'neutral'
        else:
            return 'negative'

    def on_data(self, data):
        with open("LOCAL_FILE_PATH", 'ab+') as f:
            data = json.loads(data)
            Kafka_Producer.kafkaProducer(data)
            red.sadd("MetaData", data['text'])
        return True

    def on_error(self, status):
        print("ERROR", status)


try:
    if __name__ == '__main__':
        tp = TwitterHandler()
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = tweepy.Stream(auth, tp)
        stream.filter(track=filter_Words_Lists)

except Exception as e:
    print("Twitterexception", e)

finally:
    if __name__ == '__main__':
        tp = TwitterHandler()
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = tweepy.Stream(auth, tp)
        stream.filter(track=filter_Words_Lists)
