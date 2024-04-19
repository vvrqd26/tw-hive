import asyncio
import time


async def sleep():
    time.sleep(1)


async def func1():
    print('func1')
    sleep()


async def func2():
    print('func2')
    sleep()


async def main():
    print('start')

    start = time.time()
    task1 = asyncio.create_task(func1())
    task2 = asyncio.create_task(func2())
    await task1
    await task2
    print(f'time: {time.time() - start}')
    print('end')

if __name__ == '__main__':
    asyncio.run(main())
