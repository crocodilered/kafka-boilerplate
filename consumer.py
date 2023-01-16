import argparse
import asyncio

from lib import Consumer


async def consume(msg):
    print(
        'Consumed message: ',
        msg.topic,
        msg.partition,
        msg.offset,
        msg.key,
        msg.value,
        msg.timestamp
    )


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('topic')
    parser.add_argument('group')
    args = parser.parse_args()
    topic, group = args.topic, args.group

    consumer = Consumer(topic, group)
    await consumer.start()

    try:
        async for msg in consumer:
            await consume(msg)
    finally:
        await consumer.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
