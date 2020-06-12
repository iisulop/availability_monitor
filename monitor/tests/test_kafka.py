import datetime
import os

import pytest

from monitor.handlers.kafka import Kafka
from monitor.handlers.handler import Handler


@pytest.fixture(scope="session")
def kafka_topic():
    return "test-topic-1"


@pytest.fixture(scope="session")
def kafka_connect_data(kafka_topic):
    return dict(
        host='localhost:9092',
        topic=kafka_topic,
        client_id='test client #1',
        security_protocol='PLAINTEXT',
    )


@pytest.fixture(scope="class")
def kafka_producer(kafka_connect_data):
    return Kafka(**kafka_connect_data)


@pytest.mark.skipif(os.environ.get("CI", None) is not None, reason="No Kafka instance available in CI")
class TestKafka:
    def test_init(self, kafka_connect_data):
        kafka = Kafka(**kafka_connect_data)
        assert isinstance(kafka, Kafka)
        assert isinstance(kafka, Handler)

    def test_send_result(self, kafka_producer):
        msg = dict(
            result="ok",
            url="https://urlgoeshere.com/page",
            response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
            status_code=200,
        )
        kafka_producer.send_msg(msg)
