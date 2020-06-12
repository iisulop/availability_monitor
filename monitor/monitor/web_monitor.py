import datetime
import re
from typing import Optional

import requests
from apscheduler.schedulers.background import BackgroundScheduler

from monitor.handlers.handler import Handler


class WebMonitor:
    """
    `WebMonitor` object handling (periodic) checking of the status of a web page.
    """
    def __init__(
            self,
            url: str,
            handler: Handler,
            regex: Optional[str] = None,
            period: Optional[float] = None,
            scheduler: Optional[BackgroundScheduler] = None
    ):
        """
        Initialize WebMonitor.

        :param url: Url whose status to check
        :param handler: Producer to which pass the result of the check
        :param regex: Optional regex to check for in the response body and report the result of the check
        :param period: Delay between triggering the checks in seconds. Can be omitted for one-off checkcs
        :param scheduler: `BackgrroundScheduler` object to use for periodic checks. Can be omitted for one-off checks
        """
        self._url = url
        self._regex = regex
        if handler is not None:
            self._handler = handler
        if period is not None:
            self._period = period
        if scheduler is not None:
            self._scheduler = scheduler

    def check(self):
        """
        Check the status of the web page and pass the result to the producer
        """
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

        self._handler.send_msg(message)

    def flush(self, timeout: Optional[float] = None):
        """
        Flush the messages queued for processing in the handler.

        :param timeout: Optional timeout in seconds to wait before giving up
        """
        self._handler.flush(timeout)

    def schedule(self):
        """
        Schedule the web page check operation with `period` time apart.

        :raises: ValueError if `scheduler` or `period` is not given in `__init__`
        """
        if self._period is None:
            raise ValueError("No `period` parameter passed in `__init__`. Scheduled runs not supported")
        if self._scheduler is None:
            raise ValueError("No `scheduler` parameter passed in `__init__`. Scheduled runs not supported")

        self.check()
        self._scheduler.add_job(self.check, 'interval', seconds=self._period)
