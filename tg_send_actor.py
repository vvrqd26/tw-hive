from actor import Actor
from tweety.types import usertweet
from filter import filter_tweet_by_gt_day
from extractor import extract_data, extract_tags


class TGSendActor(Actor):
    def __init__(self):
        super().__init__()

    def run(self):
        while True:
            message = self.recv()
            print(
                f"Sending tweet: {message['text']} {message['url']} {message['tags']} {message['ca']}")


class TWFilterActor(Actor):
    def __init__(self, sender: Actor):
        super().__init__()
        self.sender = sender

    def run(self):
        while True:
            message = self.recv()

            # 过滤掉超过1天的推文
            filtered_tweets = filter_tweet_by_gt_day(message, 1)

            # 提取推文数据和标签
            for tweet in filtered_tweets:
                ca = extract_data(tweet['text'])
                tags = extract_tags(tweet['text'])

                # 发送推文数据和标签给TGSendActor
                self.sender.send({
                    'text': tweet['text'],
                    'url': tweet['url'],
                    'tags': tags,
                    'ca': ca
                })
