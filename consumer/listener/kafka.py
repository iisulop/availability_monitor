from typing import List, Union

from kafka import KafkaConsumer

from consumer.listener.consumer.consumer import Consumer


class Kafka:
    def __init__(
            self,
            consumer: Consumer,
            topic: str,
            bootstrap_servers: Union[List[str], str],
            client_id: str,
            security_protocol: str,
            ssl_certfile: str,
            ssl_keyfile: str
    ):
        self._consumer = consumer
        self._connection = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            security_protocol=str,
            ssl_certfile=str,
            ssl_keyfile=str,
            # Supports `value_deserializer`
        )

    def listen(self):
        for msg in self._connection:
            self._consumer.handle(msg)
