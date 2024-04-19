import asyncio
import gradio as gr
from nats import connect
import queue

nats_server = 'nats://hive1234:hive5678@107.182.188.42:4222'

q = queue.Queue()


def natsClient(server_url, subject, msg):
    async def f():
        nc = await connect(server_url)
        await nc.publish(subject, msg.encode())
        await nc.drain()
    asyncio.run(f())


client = gr.Interface(natsClient, inputs=[gr.Text(nats_server), 'text', 'textarea'],
                      outputs=None, title="NATS 消息发送")


client.launch()
