import argparse
import asyncio
from random import choice
from typing import Dict

from lib import Producer


def generate_payload() -> Dict:
    names = ('Вася', 'Петя', 'Маша', 'Глаша', 'Диана')
    ages = [21, 22, 24, 35, 54]
    return {
        'name': choice(names),
        'age': choice(ages)
    }


async def send_message(topic: str):
    producer = Producer(topic)
    await producer.start()
    try:
        response = await producer.push('register', generate_payload())
        print(f'Message sent: {response}')
    finally:
        await producer.shutdown()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('topic')
    args = parser.parse_args()
    asyncio.run(send_message(args.topic))


if __name__ == '__main__':
    main()
