import datetime
import json
import re

import requests

from availability_monitor.monitor.producer.kafka import Kafka
from availability_monitor.monitor.producer.producer import Producer


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
                timestamp=datetime.datetime.now(tz=datetime.timezone.utc)
            )
            if 200 <= result.status_code < 300:
                message["result"] = "ok"
            else:
                message["result"] = "error"
            if self._regex:
                regex_result = re.search(self._regex, result.text)
                message["found_match"] = {self._regex: regex_result is not None}
        except ConnectionError:
            result = "Failed to connect"
            message = dict(
                result="failure",
            )

        self._producer.send_result(json.dumps(message))
