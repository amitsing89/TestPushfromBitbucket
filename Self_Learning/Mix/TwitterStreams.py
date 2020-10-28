import re
import tweepy
import json
from textblob import TextBlob
import Kafka_Producer
import redis
import time

access_token_secret = 'pH3iIS4SNosfNYv9XVKmwSsLmT0hLjPlInO1iYS9f33Lu'
consumer_key = 'PzN3rCjnxaD7bbkKh7hUa8jb5'
consumer_secret = 'kepRSnVC7XzNbjs26DgVBczsWrJDCoLO2ascB8746zl3Ewxvp6'
access_token = '286178494-qdWjmeosEfVEbSHAVB3iJmwBicvgYiOIs4Xf0vOC'

FILTER_WORDS_LIST = ['#BreakingNews', '#Market', '#Metoo']
LOCAL_FILE_PATH = 'TweetDatastorage.json'

red = redis.Redis(host='localhost')


class TwitterHandler(tweepy.StreamListener):
    def on_data(self, data):
        with open(LOCAL_FILE_PATH, 'ab+') as f:
            tweets = []
            parsed_tweet = {}
            data = json.loads(data)
            time.sleep(5)
            Kafka_Producer.kafkaProducer(data)
            # print "Structured Streamed Data",data
            # data['sentiment'] = self.get_tweet_sentiment(data['text'])
            # parsed_tweet['text'] = data['text']
            # parsed_tweet['sentiment'] = self.get_tweet_sentiment(data['text'])
            # tweets.append(parsed_tweet)
            # if data['retweet_count'] > 0:
            # if tweet has retweets, ensure that it is appended only once
            # if parsed_tweet not in tweets:
            #     try:
            #         # Kafka_Producer.kafkaProducer(parsed_tweet['text'].encode('utf-8'))
            #         Kafka_Producer.kafkaProducer(data)
            #         tweets.append(parsed_tweet)
            #     except Exception as e:
            #         print(e)
            #     finally:
            #         red.sadd("MetaData", parsed_tweet)
            # else:
            #     try:
            #         Kafka_Producer.kafkaProducer(data)
            #         f.write(data['text'].encode('utf-8') + "\n".encode('utf-8'))
            #     except Exception as e:
            #         print(e)
            #     finally:
            #         red.sadd("MetaData", parsed_tweet)
        return True

    def on_error(self, status):
        print("ERROR", status)


try:
    if __name__ == '__main__':
        tp = TwitterHandler()
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = tweepy.Stream(auth, tp)
        stream.filter(track=FILTER_WORDS_LIST)

except Exception as e:
    print("Twitterexception", e)

finally:
    if __name__ == '__main__':
        tp = TwitterHandler()
        auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(access_token, access_token_secret)
        stream = tweepy.Stream(auth, tp)
        stream.filter(track=FILTER_WORDS_LIST)
