import json
from typing import Dict

from aiokafka import AIOKafkaProducer, AIOKafkaConsumer


class Producer(AIOKafkaProducer):
    def __init__(self, topic: str):
        self.__topic = topic
        super().__init__(
            bootstrap_servers='localhost:9092',
            key_serializer=self.__serialize_key,
            value_serializer=self.__serialize_value
        )

    @classmethod
    def __serialize_key(cls, key: str) -> bytes:
        return bytes(key.encode('utf-8'))

    @classmethod
    def __serialize_value(cls, value: Dict) -> bytes:
        s = json.dumps(value)
        return bytes(s.encode('utf-8'))

    def push(self, key: str, value: Dict):
        return self.send_and_wait(self.__topic, value, key)

    async def shutdown(self):
        await self.stop()


class Consumer(AIOKafkaConsumer):
    def __init__(self, topic: str, group: str):
        super().__init__(
            topic,
            bootstrap_servers='localhost:9092',
            group_id=group,
            key_deserializer=self._deserialize_key,
            value_deserializer=self._deserialize_value
        )

    @classmethod
    def _deserialize_key(cls, raw: bytes) -> str:
        return raw.decode('utf-8')

    @classmethod
    def _deserialize_value(cls, raw: bytes) -> str:
        return json.loads(raw.decode('utf-8'))

    async def shutdown(self):
        await self.stop()
