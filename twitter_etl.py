import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs


def run_twitter_etl():
    access_key = "v8YkQeBhfuzdSY1Lm9GSjQshF"
    access_secret = "41Xlwg9OkXabmZQgZuc0R7fb05bIgltX3Po8OZDIX66mGD2YKz"
    consumer_key = "710835349203779584-uUEGZZdSa1EXHGZeFlj149MseI3J4xs"
    consumer_secret = "d1GaJLAwRzxq4z6ujuUZNgvqJwn9Lq9oqoC7K94bKNkm8"

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # # # Creating an API object
    api = tweepy.API(auth)
    tweets = api.user_timeline(screen_name='@elonmusk',
                               # 200 is the maximum allowed count
                               count=200,
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )

    list = []
    for tweet in tweets:
        text = tweet._json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('refined_tweets.csv')