import time
from hive_typing import AuthType
from tweety import Twitter
from typing import Union
import asyncio
from lg import lg

"""
Ant class
单个工作爬虫，负责爬取单个工作信息
使用后空闲一段指定时间后，才能再次使用
"""


class Ant:
    def __init__(self, name: str, auth: AuthType, sleep_time: float = 0.001):
        self.auth = auth
        self.sleep_time = sleep_time
        self.is_idle = True
        self.last_use_time = 0
        self.name = name

        try:
            client = Twitter(name)
            print(f'登录:{name}')
            client.load_auth_token(str(auth)) if isinstance(
                auth, str) else client.sign_in(username=auth[0], password=auth[1])
            self.client = client
        except Exception as e:
            print(f'{name}登录失败')
            self.client = None

    @classmethod
    async def create(cls, name: str, auth: AuthType, sleep_time: float = 0.001):
        return await asyncio.get_event_loop().run_in_executor(None, cls, name, auth, sleep_time)

    def use(self) -> Union[Twitter, None]:
        if not self.is_idle or time.time() - self.last_use_time < self.sleep_time:
            raise None
        self.is_idle = False
        self.last_use_time = time.time()
        return self.client

    def release(self):
        self.is_idle = True
        self.last_use_time = time.time()
