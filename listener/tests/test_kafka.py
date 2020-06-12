import os

import pytest
from apscheduler.schedulers.blocking import BlockingScheduler

from listener.handlers.handler import Handler
from listener.kafka_consumer import Kafka


class MockProducer(Handler):
    def __init__(self):
        self.messages = []

    def handle(self, msg):
        self.messages.append(msg)


@pytest.fixture(scope="session")
def kafka_topic():
    return "test-topic-1"


@pytest.fixture(scope="session")
def kafka_connect_data(kafka_topic):
    return dict(
        producer=MockProducer(),
        topic=kafka_topic,
        bootstrap_servers='localhost:9092',
        scheduler=BlockingScheduler,
        client_id='test client #1',
        security_protocol='PLAINTEXT',
    )


@pytest.mark.skipif(os.environ.get("CI", None) is not None, reason="No Postgresql DB available in CI")
class TestKafka:
    def test_init(self, kafka_connect_data):
        Kafka(**kafka_connect_data)
