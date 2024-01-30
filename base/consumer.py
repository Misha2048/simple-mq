import asyncio
import json
from typing import NoReturn

import redis

from mq.base.schema import BaseSchema


class BaseConsumer:
    topic: str

    def __init__(self) -> NoReturn:
        self.__queue: asyncio.Queue = asyncio.Queue()
        self.__schema_class = self.handle_message.__annotations__.get('schema')

    async def __get_messages(self) -> NoReturn:
        r = redis.Redis()
        while True:
            json_messages = r.lrange(self.topic, 0, -1)
            for json_message in json_messages:
                await self.__queue.put(json.loads(json_message))
                r.lpop(self.topic)
            await asyncio.sleep(0.001)      

    async def __handle_messages(self) -> NoReturn:
        while True:
            message = await self.__queue.get()
            asyncio.create_task(self.handle_message(
                schema=self.__schema_class.model_validate(message) if self.__schema_class else message
            ))
            await asyncio.sleep(0.001)
    
    async def run(self) -> NoReturn:
        await asyncio.gather(
            self.__handle_messages(),
            self.__get_messages(),
        )

    async def handle_message(self, schema: BaseSchema) -> NoReturn:
        ...