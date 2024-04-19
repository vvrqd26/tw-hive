import pytest
from filter import filter_tweet_by_gt_day
from datetime import datetime, timedelta
from tg_send_actor import TWFilterActor, TGSendActor


def test_filter_tweets():
    tweets = [
        {'id': 1, 'text': 'Tweet 1 0x7900a081E80Eab4503F4B8C5Ef1537c491b522D9',
            'created_on': 'Mon, 08 Apr 2024 12:49:49 GMT', 'url': '111'},
        {'id': 2, 'text': 'Tweet 2 WL',
            'created_on': 'Mon, 09 Apr 2024 12:49:49 GMT', 'url': '222'},
        {'id': 3, 'text': 'Tweet 3 airdrop',
            'created_on': 'Mon, 10 Apr 2024 12:49:49 GMT', 'url': '333'},
        {'id': 5, 'text': 'Tweet 5 0x7900a081E80Eab4503F4B8C5Ef1537c491b522D9',
            'created_on': datetime.now()-timedelta(days=1), 'url': '444'},
        {'id': 6, 'text': 'Tweet 6', 'created_on': datetime.now().strftime(
            '%a, %d %b %Y %H:%M:%S GMT'), 'url': '555'},
    ]
    tw1 = filter_tweet_by_gt_day(tweets, 1)
    assert len(tw1) == 2
    assert tw1[0]['id'] == 5
    assert tw1[1]['id'] == 6

    sender = TGSendActor()
    sender.start()
    ft = TWFilterActor(sender)
    ft.start()

    ft.send(tweets)

    ft.join()
