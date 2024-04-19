import asyncio
import nats
from queue import Queue
import threading
from lg import lg
from nats.aio.client import Client

server_url = "nats://hive1234:hive5678@107.182.188.42:4222"


def publish_message(subject: str, message: str):
    asyncio.run(publish_message_async(subject, message))


async def publish_message_async(subject: str, message: str):
    nc = await nats.connect(server_url)
    await nc.publish(subject, message.encode())
    await nc.drain()


class MsgSender:
    def __init__(self):
        self.q = []
        self.nc: Client = None
        self.status = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if (self.status == 0):
            self.send()

    def push(self, subject: str, message: str):
        self.q.append((subject, message))

    def send(self):
        async def send_async():
            self.nc = await nats.connect(server_url)
            for (subject, message) in self.q:
                await self.nc.publish(subject, message.encode())
            await self.nc.drain()
            await self.nc.close()
        asyncio.run(send_async())
        self.status = 1


def open_msg_sender():
    return MsgSender()
