import asyncio
import json
import os
import random
from hive import Hive
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
import re
from lg import lg
from nats_client import open_msg_sender, publish_message
from extractor import extract_data
from filter import filter_tweet_by_gt_day

h = Hive()
h.run()


def load_kol_list(file_name='kol.csv'):
    koldf = pd.read_csv(file_name, header=None, names=[
                        'name', 'last_check_time'])
    koldf = koldf.fillna('')
    return koldf


def load_watch_users_list(file_name='watch_users.csv'):
    watch_users_df = pd.read_csv(file_name, header=None, names=[
        'name', 'last_check_time'])
    watch_users_df = watch_users_df.fillna('')
    return watch_users_df


def save_kol_list(koldf, file_name='kol.csv'):
    koldf.to_csv(file_name, index=False, header=False)


def save_watch_users_list(watch_users_df, file_name='watch_users.csv'):
    watch_users_df.to_csv(file_name, index=False, header=False)


def get_followings(account: str):
    account = account.strip()
    filename = f'datas/users/{account}.csv'
    ant = h.get_idle_ant()
    status = False

    if not ant:
        print(
            f'{ant.name}[{pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}]: no idle ant')
        return

    print(
        f'{ant.name}[{pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}]: getting {account} followings')
    try:
        followings = ant.client.get_user_followings(account)
        watch_users = load_watch_users_list()
        if followings:
            users = pd.DataFrame(followings.users)
            users['collection_date'] = pd.Timestamp.now().strftime(
                '%Y-%m-%d %H:%M:%S')
            users['tags'] = ''
            users['checked'] = False
            users['check_time'] = ''

            # 读取上一次记录从csv,如果存在，合并datafame 相同 id 只保留最新的记录
            last_record = pd.read_csv(
                filename) if os.path.exists(filename) else pd.DataFrame()
            if last_record.empty == False:
                users = pd.concat([last_record, users]).drop_duplicates(
                    subset='id', keep='first')
                new_users = users[users['id'].isin(last_record['id']) == False]
                new_watch_users = pd.DataFrame(
                    columns=['name', 'last_check_time'])
                new_watch_users['name'] = new_users[['username']]
                new_watch_users['last_check_time'] = ''
                new_watch_users = pd.concat(
                    [watch_users, new_watch_users]).drop_duplicates(subset=['name'])
                new_watch_users.to_csv(
                    'watch_users.csv', index=False, header=False)
            # 保存到csv
            users.to_csv(filename, index=False)
            status = True
    except Exception as e:
        print(
            f'{ant.name}[{pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}]: get {account} followings error: {e}')
    finally:
        h.return_ant(ant)

    return status


def get_user_tweets(account: str):
    account = account.strip()
    filename = f'./datas/tweets/{account}.csv'
    ant = h.get_idle_ant()
    status = False
    if not ant:
        return
    print(
        f'{ant.name}[{pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}]: getting {account} tweets')
    try:
        tweetsResult = ant.client.get_tweets(account)
        if tweetsResult and len(tweetsResult.tweets) > 0:
            tweets = pd.DataFrame(tweetsResult.tweets)
            tweets['collection_date'] = pd.Timestamp.now().strftime(
                '%Y-%m-%d %H:%M:%S')
            tweets.to_csv(filename, index=False)
            ethereum_address_pattern = r'0x([A-Fa-f0-9]{40})'
            tags = ['whitelist', 'airdrop', 'whitelists', 'wl', 'launch','live','test']
            # 匹配标签
            tags_pattern = r'\b(' + '|'.join(tags) + r')\b'
            ndf1 = tweets[['id', 'text', 'author',
                           'media', 'url', 'created_on']]
            ndf1['created_on'] = pd.to_datetime(ndf1['created_on'])
            # 过滤掉大于1天的
            ndf1 = ndf1[ndf1['created_on'] >=
                        pd.Timestamp.now(tz='UTC') - pd.Timedelta(hours=2)]
            ndf1['ca'] = ndf1['text'].str.extract(
                ethereum_address_pattern, flags=re.MULTILINE)
            ndf1['other'] = ndf1['text'].str.extract(
                tags_pattern, flags=re.IGNORECASE)
            ndf1 = ndf1[~ndf1['ca'].isna() | ~ndf1['other'].isna()]
            ndf1 = ndf1.fillna('')

            with open_msg_sender() as sender:
                for index, row in ndf1.iterrows():
                    sender.push('tgmsg', json.dumps({
                        "type": "new_tweet",
                        "from_account": account,
                        "tweet": {
                            "text": row[1],
                            "address": row[6],
                            "tg": "",
                            "url": row[4],
                            "tags": row[7],
                        }
                    }))

            status = True
    except Exception as e:
        print(
            f'{ant.name}[{pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}]: get {account} tweets error: {e}')
    finally:
        h.return_ant(ant)

    return status


def get_following_all():
    koldf = load_kol_list()

    for index, account in enumerate(koldf['name']):
        # 检查上次检查时间,如果小于30分钟 跳过
        if (pd.Timestamp.now() - pd.Timestamp(
                koldf.at[index, 'last_check_time'])).seconds < 120 * 60:
            continue
        status = get_followings(account)
        koldf.at[index, 'last_check_time'] = pd.Timestamp.now().strftime(
            '%Y-%m-%d %H:%M:%S') if status else koldf.at[index, 'last_check_time']
        sleep_time = 2 * random.random()
        koldf.to_csv('kol.csv', index=False, header=False)
        time.sleep(sleep_time)


def get_user_tweets_all():
    watch_users = load_watch_users_list()
    for index, account in enumerate(watch_users['name']):
        # 检查上次检查时间,如果小于30分钟 跳过
        if (pd.Timestamp.now() - pd.Timestamp(
                watch_users.at[index, 'last_check_time'])).seconds < 60 * 60:
            continue
        status = get_user_tweets(account)
        if status:
            watch_users.at[index, 'last_check_time'] = pd.Timestamp.now().strftime(
                '%Y-%m-%d %H:%M:%S')
        else:
            watch_users.drop(index, inplace=True)

        sleep_time = 2 * random.random()
        time.sleep(sleep_time)
        watch_users.to_csv('watch_users.csv', index=False, header=False)


def get_all_task():
    count = 0
    while True:
        count += 1
        print(f'第{count}次检查')
        get_following_all()
        get_user_tweets_all()
        time.sleep(10)


if __name__ == '__main__':
    get_all_task()
