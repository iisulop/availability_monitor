from abc import abstractmethod, ABC


# pylint: disable=too-few-public-methods
from kafka.consumer.fetcher import ConsumerRecord


class Handler(ABC):
    """
    Consumer class definition for handling messages
    """
    @abstractmethod
    def handle(self, msg: ConsumerRecord):
        """
        Handle the information in msg.

        :param msg: The message from Kafka
        """
