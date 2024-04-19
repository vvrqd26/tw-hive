import asyncio
from typing import List, Union, Tuple
from hive_typing import AuthType
from ant import Ant
import asyncio
from lg import lg
import random

import csv


class Hive:
    def __init__(self, AuthList: List[AuthType] = []):
        self.pools = []
        for index, auth in enumerate(AuthList):
            self.pools.append(Ant(name=f'ant_{index}', auth=auth))

    def get_idle_ant(self) -> Union[Ant, None]:
        try:
            ant = self.pools.pop()
            return ant
        except asyncio.TimeoutError:
            print('没有空闲的ant')
            return None

    def return_ant(self, ant: Ant):
        ant.release()
        self.pools.insert(0, ant)

    async def load_account_csv(self, path='./account.csv'):
        f = open(path, 'r')
        reader = csv.reader(f)
        tasks = []
        for row in reader:
            username = row[1]
            token = row[0]

            tasks.append(asyncio.create_task(
                Ant.create(name=f'ant_{username}', auth=token)))

        ants = await asyncio.gather(*tasks)
        for ant in ants:
            if ant.client == None:
                continue
            self.pools.append(ant)

        f.close()

    def run(self):
        asyncio.run(self.load_account_csv())
        self.shuffle()

    # 打乱队列顺序
    def shuffle(self):
        random.shuffle(self.pools)

    def execute(self, func, *args, **kwargs):
        ant = self.get_idle_ant()
        result = None
        if ant == None:
            return
        func = getattr(ant.client, func)
        if func == None:
            return
        try:
            result = func(*args, **kwargs)
            return result

        except Exception as e:
            print(e)
        finally:
            self.return_ant(ant)
