from tweety.types import usertweet
import pandas as pd


def handler_tweet(result: usertweet.UserTweets):
    tweets = result.tweets


def filter_tweet_by_gt_day(tweets: list, day=1):
    # 过滤掉大于指定天数的推文
    filtered_tweets = []
    for tweet in tweets:
        create_on = pd.to_datetime(tweet['created_on'])
        if (pd.Timestamp.now() - create_on).days <= day:
            filtered_tweets.append(tweet)
    return filtered_tweets
