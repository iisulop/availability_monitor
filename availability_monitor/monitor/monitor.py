import datetime
import re

import requests

from availability_monitor.monitor.producers.kafka import Kafka
from availability_monitor.monitor.producer import Producer


class Monitor:
    def __init__(self, url: str, regex: str = None, producer: Producer = None):
        """

        :param url:
        :param regex:
        :param producer:
        """
        self._url = url
        self._regex = None
        if regex is not None:
            self._regex = regex
        self._producer = Kafka
        if producer is not None:
            self._producer = producer

    def check(self):
        try:
            result = requests.get(self._url)
            message = dict(
                url=self._url,
                status_code=result.status_code,
                response_time=datetime.datetime.now(tz=datetime.timezone.utc).isoformat(),
                elapsed=result.elapsed.seconds + (result.elapsed.microseconds / (1000 * 1000)),
            )
            if 200 <= result.status_code < 300:
                message["result"] = "ok"
            else:
                message["result"] = "error"
            if self._regex:
                regex_result = re.search(self._regex, result.text)
                message["regex_matches"] = {self._regex: regex_result is not None}
        except ConnectionError:
            message = dict(
                url=self._url,
                result="failure",
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc).isoformat()
            )

        self._producer.send_result(message)

    def flush_producer(self, timeout: float = None):
        self._producer.flush()
