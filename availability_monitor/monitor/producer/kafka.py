import json
import logging

from kafka.errors import KafkaTimeoutError

from availability_monitor.monitor.producer import Producer
from kafka import KafkaProducer

logger = logging.Logger(__name__)


class Kafka(Producer):
    _PRODUCER = None

    def __init__(
            self,
            address: str,
            topic: str,
            client_id: str,
            security_protocol: str,
            ssl_certfile: str,
            ssl_keyfile: str,
    ):
        """

        :param address: Kafka server address in format `localhost:1234`
        """
        self._topic = topic
        if self._PRODUCER is None:
            self._PRODUCER = KafkaProducer(
                bootstrap_servers=address,
                client_id=client_id,
                # Supports `value_serializer`
            )

    def _serialize(self, msg: dict) -> str:
        return json.dumps(msg)

    def send_result(self, msg: dict):
        try:
            self._PRODUCER.send(self._topic, self._serialize(msg))
        except KafkaTimeoutError as e:
            logger.error('Kafka timed out with error %s', e)
