import asyncio

from mq.base.consumer import BaseConsumer


async def init_and_run_consumers(consumers: list[type[BaseConsumer]] = None):
    source = consumers
    if not consumers:
        source = BaseConsumer.__subclasses__()
    consumers = [consumer_class() for consumer_class in source]
    await asyncio.gather(*[consumer.run() for consumer in consumers])


async def run_consumers(consumers: list[BaseConsumer]):
    await asyncio.gather(*[consumer.run() for consumer in consumers])