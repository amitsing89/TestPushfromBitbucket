import time
import json
import twitter
import dateutil.parser

from pyspark import SparkContext
from pyspark.streaming import StreamingContext


class Tweet(dict):
    """
        Classe dictionnaire pour initialiser un dictionnaire avec les informations d'un tweet.
    """

    def __init__(self, tweet_in, encoding='utf-8'):
        super(Tweet, self).__init__(self)
        if tweet_in and 'delete' not in tweet_in:
            self['id'] = tweet_in['id']
            self['geo'] = tweet_in['geo']['coordinates'] if tweet_in['geo'] else None
            self['text'] = tweet_in['text'].encode(encoding)
            self['user_id'] = tweet_in['user']['id']
            self['hashtags'] = [x['text'].encode(encoding) for x in tweet_in['entities']['hashtags']]
            self['timestamp'] = dateutil.parser.parse(tweet_in[u'created_at']).replace(tzinfo=None).isoformat()
            self['screen_name'] = tweet_in['user']['screen_name'].encode(encoding)


def connect_twitter():
    consumer_key = "pQCGWuPKWEXb35DWrMzeJSaQB"
    consumer_secret = "OLJPMoooSFaaAK6tF1Vd57I56Vm8pM6f7lkyryp4gBrSBsIyhc"
    access_token = "4713676566-ge3CgK0iTXTzsZAsWq337xGTtVgmKFwqH5lVq3w"
    access_secret = "Owcffjpl5TfVU2NI4xR6bdyfv2gQ9CQBQeAZCcdgIlUoI"
    auth = twitter.OAuth(token=access_token,
                         token_secret=access_secret,
                         consumer_key=consumer_key,
                         consumer_secret=consumer_secret)
    return twitter.TwitterStream(auth=auth)


def get_next_tweet(twitter_stream):
    """
    Return : JSON
    """
    block = False  # True
    stream = twitter_stream.statuses.sample(block=False)
    tweet_in = None
    while not tweet_in or 'delete' in tweet_in:
        tweet_in = stream.next()
        tweet_parsed = Tweet(tweet_in)

    return json.dumps(tweet_parsed)


def process_rdd_queue(twitter_stream, nb_tweets=5):
    """
     Create a queue of RDDs that will be mapped/reduced one at a time in 1 second intervals.
    """
    rddQueue = []
    for i in range(nb_tweets):
        json_twt = get_next_tweet(twitter_stream)
        dist_twt = ssc.sparkContext.parallelize([json_twt], 5)
        print("TEST", dist_twt)
        rddQueue += [dist_twt]

    lines = ssc.queueStream(rddQueue, oneAtATime=False)
    print("CHECK", lines.pprint())


sc = SparkContext(appName="PythonStreamingQueueStream")
ssc = StreamingContext(sc, 30)

twitter_stream = connect_twitter()
process_rdd_queue(twitter_stream)
# try:
#     ssc.stop(stopSparkContext=True, stopGraceFully=True)
# except:
#     pass

ssc.start()
ssc.awaitTermination()
# time.sleep(2)
# ssc.stop(stopSparkContext=True, stopGraceFully=True)
