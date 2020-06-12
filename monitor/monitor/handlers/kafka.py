import json
import logging
from typing import Dict, Any, Optional, List

from kafka.admin import NewTopic
from kafka.errors import KafkaTimeoutError, TopicAlreadyExistsError, UnrecognizedBrokerVersion
from kafka import KafkaProducer, KafkaAdminClient

from monitor.handlers.handler import Handler

logger = logging.Logger(__name__)


class Kafka(Handler):
    """
    Kafka producer class for sending messages to the given topic in the given Kafka instance.
    """
    _producer = None

    @staticmethod
    def init_params() -> List[str]:
        """
        Get the parameters required by Kafka `__init__`.
        :return: List of required parameters
        """
        return [
            "host",
            "topic",
            "client_id",
            "security_protocol",
            "ssl_cafile",
            "ssl_certfile",
            "ssl_keyfile",
        ]

    def __init__(
            self,
            host: str,
            topic: str,
            client_id: str,
            security_protocol: Optional[str] = None,
            ssl_cafile: Optional[str] = None,
            ssl_certfile: Optional[str] = None,
            ssl_keyfile: Optional[str] = None,
    ):
        """
        Initialize Kafka object.

        :param host: Kafka server address in format `localhost:1234`
        :param topic: Topic to which the messages will be sent.
                      If the topic does not exist during initialization it will be created.
        :param client_id: Client id for recognizing this producer
        :param security_protocol: Security protocol to use with Kafka ('SSL')
        :param ssl_cafile: CA file
        :param ssl_certfile: Certificate file
        :param ssl_keyfile: Key file
        """
        self._host = host
        self._topic = topic
        if self._producer is None:
            self._producer = KafkaProducer(
                bootstrap_servers=self._host,
                client_id=client_id,
                security_protocol=security_protocol,
                ssl_cafile=ssl_cafile,
                ssl_certfile=ssl_certfile,
                ssl_keyfile=ssl_keyfile,
                # Supports `value_serializer`
            )
        self._create_topic(topic)

    def _create_topic(self, name: str):
        try:
            client = KafkaAdminClient(bootstrap_servers=self._host)
        except UnrecognizedBrokerVersion as e:
            logger.warning("Could not create topic %s due to %s", name, e)
            return

        topic = NewTopic(name, 1, 1)
        try:
            client.create_topics([topic])
        except TopicAlreadyExistsError:
            pass

    @staticmethod
    def _serialize(msg: dict) -> str:
        return json.dumps(msg).encode('utf-8')

    def send_msg(self, message: Dict[Any, Any]):
        """
        Send the given `message` to the Kafka instance and topic.

        :param message: `dict` of information that the `_serialize` method is able to serialize.
        """
        try:
            self._producer.send(self._topic, self._serialize(message))
        except KafkaTimeoutError as e:
            logger.error('Kafka timed out with error %s', e)

    def flush(self, timeout: Optional[float] = None):
        """
        Force send any queued messages.

        :param timeout: Optional timeout in seconds to wait before giving up
        :raises: KafkaTimeoutError if the queue was not flushed before the given timeout
        """
        self._producer.flush(timeout=timeout)
