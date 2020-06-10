import json
import logging

from kafka.admin import NewTopic
from kafka.errors import KafkaTimeoutError, TopicAlreadyExistsError
from kafka import KafkaProducer, KafkaAdminClient

from availability_monitor.monitor.producer import Producer

logger = logging.Logger(__name__)


class Kafka(Producer):
    _PRODUCER = None

    @staticmethod
    def init_params():
        return [
            'host',
            'topic',
            'client_id',
        ]

    def __init__(
            self,
            host: str,
            topic: str,
            client_id: str,
            security_protocol: str = None,
            ssl_certfile: str = None,
            ssl_keyfile: str = None,
    ):
        """

        :param host: Kafka server address in format `localhost:1234`
        """
        self._host = host
        self._topic = topic
        if self._PRODUCER is None:
            self._PRODUCER = KafkaProducer(
                bootstrap_servers=self._host,
                client_id=client_id,
                # Supports `value_serializer`
            )
        self._create_topic(topic)

    def _create_topic(self, name: str):
        client = KafkaAdminClient(bootstrap_servers=self._host)
        topic = NewTopic(name, 1, 1)
        try:
            client.create_topics([topic])
        except TopicAlreadyExistsError:
            pass

    def _serialize(self, msg: dict) -> str:
        return json.dumps(msg).encode('utf-8')

    def send_result(self, msg: dict):
        try:
            self._PRODUCER.send(self._topic, self._serialize(msg))
        except KafkaTimeoutError as e:
            logger.error('Kafka timed out with error %s', e)

    def flush(self, timeout: float = None):
        self._PRODUCER.flush(timeout=timeout)
