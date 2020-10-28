import re
import tweepy
from tweepy import OAuthHandler
from textblob import TextBlob

LOCAL_FILE_PATH = 'TweetData.json'
f = open(LOCAL_FILE_PATH, 'wb')


class TwitterClient(object):
    '''
    Generic Twitter Class for sentiment analysis.
    '''

    def __init__(self):
        '''
        Class constructor or initialization method.
        '''
        # keys and tokens from the Twitter Dev Console
        access_token_secret = 'pH3iIS4SNosfNYv9XVKmwSsLmT0hLjPlInO1iYS9f33Lu'
        consumer_key = 'PzN3rCjnxaD7bbkKh7hUa8jb5'
        consumer_secret = 'kepRSnVC7XzNbjs26DgVBczsWrJDCoLO2ascB8746zl3Ewxvp6'
        access_token = '286178494-qdWjmeosEfVEbSHAVB3iJmwBicvgYiOIs4Xf0vOC'

        # attempt authentication
        try:
            # create OAuthHandler object
            self.auth = OAuthHandler(consumer_key, consumer_secret)
            # set access token and secret
            self.auth.set_access_token(access_token, access_token_secret)
            # create tweepy API object to fetch tweets
            self.api = tweepy.API(self.auth)
        except:
            print("Error: Authentication Failed")

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

    def get_tweets(self, query, count):
        '''
        Main function to fetch tweets and parse them.
        '''
        # empty list to store parsed tweets
        tweets = []

        try:
            # call twitter api to fetch tweets
            fetched_tweets = self.api.search(q=query, count=count)

            # parsing tweets one by one
            for tweet in fetched_tweets:
                # print(tweet)
                # empty dictionary to store required params of a tweet
                parsed_tweet = {}

                # saving text of tweet
                parsed_tweet['text'] = tweet.text
                parsed_tweet['user_name'] = tweet.user.name
                # saving sentiment of tweet
                parsed_tweet['sentiment'] = self.get_tweet_sentiment(tweet.text)

                # appending parsed tweet to tweets list
                if tweet.retweet_count > 0:
                    # if tweet has retweets, ensure that it is appended only once
                    if parsed_tweet not in tweets:
                        tweets.append(parsed_tweet)
                else:
                    tweets.append(parsed_tweet)

            # return parsed tweets
            return tweets

        except tweepy.TweepError as e:
            # print error (if any)
            print("Error : " + str(e))


def main():
    # creating object of TwitterClient Class
    api = TwitterClient()
    # calling function to get tweets
    query_list = ['#AbdulKalam']
    total_tweets = []
    for query in query_list:
        tweets = api.get_tweets(query=query, count=200)
        total_tweets.extend(tweets)
        # picking positive tweets from tweets

    # picking positive tweets from tweets
    ptweets = [tweet for tweet in total_tweets if tweet['sentiment'] == 'positive']
    # percentage of positive tweets
    positive_percentage = (100 * float(len(ptweets)) / float(len(total_tweets)))
    # picking negative tweets from tweets
    ntweets = [tweet for tweet in total_tweets if tweet['sentiment'] == 'negative']
    # picking neutral tweets
    neutraltweets = [tweet for tweet in total_tweets if tweet['sentiment'] == 'neutral']
    # percentage of negative tweets
    negative_percentage = (100 * float(len(ntweets)) / float(len(total_tweets)))
    # percentage of neutral tweets
    neut_percentage = (100 * float(len(neutraltweets)) / float(len(total_tweets)))
    print("Positive Tweets %-", positive_percentage)
    print("Negative Tweets %-", negative_percentage)
    print("Neutral Tweets %-", neut_percentage)
    # print "COMBINATION", len(total_tweets), len(ptweets), len(ntweets), len(neutraltweets)

    # adding header
    # f.write('USER' + " --> " + 'TEXT' + " --> " + 'SENTIMENT_VALUE' + "\n")

    # Appending positive tweets
    for tweet in ptweets:
        # print(tweet)
        f.write((tweet['user_name']).encode('utf-8') + "-->".encode('utf-8') + (tweet['text']).encode(
            'utf-8') + "-->".encode('utf-8') + (tweet['sentiment']).encode('utf-8'))
        f.write(((tweet['text']).encode('utf-8')) + "\n".encode('utf-8'))
    # Appending negative tweets
    for tweet in ntweets:
        # print(tweet)
        f.write((tweet['user_name']).encode('utf-8') + "-->".encode('utf-8') + (tweet['text']).encode(
            'utf-8') + "-->".encode('utf-8') + (
                    tweet['sentiment']).encode('utf-8') + "\n".encode('utf-8'))
        # f.write(((tweet['text']).encode('utf-8')))
    # Appending Negative tweets
    for tweet in neutraltweets:
        # print(tweet)
        f.write((tweet['user_name']).encode('utf-8') + "-->".encode('utf-8') + (tweet['text']).encode(
            'utf-8') + "-->".encode('utf-8') + (
                    tweet['sentiment']).encode('ascii') + "\n".encode('utf-8'))
        # f.write(((tweet['text']).encode('utf-8')))


if __name__ == "__main__":
    # calling main function
    main()
