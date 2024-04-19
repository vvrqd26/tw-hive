import nats
import asyncio

server_url = "nats://hive1234:hive5678@107.182.188.42:4222"


async def main():
    nc = await nats.connect(server_url)
    # ... use nc ...
    sub = await nc.subscribe("*")
    async for msg in sub.messages:
        print(f"Received a message: {msg.data.decode()}")

    await sub.unsubscribe()
    await nc.close()
    await nc.drain()


if __name__ == "__main__":
    asyncio.run(main())
