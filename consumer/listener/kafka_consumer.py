from typing import List, Union

from kafka import KafkaConsumer

from listener.consumer import Consumer


class Kafka:
    def __init__(
            self,
            producer: Consumer,
            topic: str,
            bootstrap_servers: Union[List[str], str],
            client_id: str,
            security_protocol: str = None,
            ssl_certfile: str = None,
            ssl_keyfile: str = None
    ):
        self._producer = producer
        self._connection = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            # security_protocol=security_protocol,
            # ssl_certfile=ssl_certfile,
            # ssl_keyfile=ssl_keyfile,
            # Supports `value_deserializer`
        )

    def listen(self):
        for msg in self._connection:
            self._producer.handle(msg)
