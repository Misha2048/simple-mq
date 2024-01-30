from typing import NoReturn

import redis

from mq.base.schema import BaseSchema


class BaseProducer:
    def produce(self, topic: str, schema: BaseSchema) -> NoReturn:
        redis.Redis().rpush(topic, schema.model_dump_json())