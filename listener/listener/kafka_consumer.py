from typing import List, Union

from apscheduler.schedulers.background import BackgroundScheduler
from kafka import KafkaConsumer

from listener.handlers.handler import Handler


# pylint: disable=too-few-public-methods

class Kafka:
    """
    Kafka consumer listening to the given topic for the given server.
    """

    def __init__(
            self,
            producer: Handler,
            topic: str,
            bootstrap_servers: Union[List[str], str],
            scheduler: BackgroundScheduler,
            client_id: str = None,
            security_protocol: str = None,
            ssl_cafile: str = None,
            ssl_certfile: str = None,
            ssl_keyfile: str = None
    ):
        """
        Initialize Kafka object.

        :param producer: Class responsible for handling the received messages
        :param topic: Kafka topic to listen to
        :param bootstrap_servers: Kafka bootstrap server address(es)
        :param client_id: Client id for recognizing this consumer
        :param scheduler: BackgroundScheduler object handling scheduling the message handling
        :param security_protocol: Security protocol to use with Kafka ('SSL')
        :param ssl_cafile: CA file
        :param ssl_certfile: Certificate file
        :param ssl_keyfile: Key file
        """
        self._producer = producer
        self._scheduler = scheduler
        self._connection = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            client_id=client_id,
            security_protocol=security_protocol,
            ssl_cafile=ssl_cafile,
            ssl_certfile=ssl_certfile,
            ssl_keyfile=ssl_keyfile,
            # Supports `value_deserializer`
        )

    def listen(self):
        """
        Start listening to the Kafka topic.
        Schedules any messages received to be processed by the given producer's `handle` method
        """
        for msg in self._connection:
            self._scheduler.add_job(self._producer.handle, args=(msg,))
