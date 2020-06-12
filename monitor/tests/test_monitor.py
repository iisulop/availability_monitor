import datetime

import pytest
import responses
from freezegun import freeze_time

from monitor.web_monitor import WebMonitor
from monitor.handlers.handler import Handler


class MockHandler(Handler):
    def __init__(self):
        self._results = []

    def send_msg(self, msg: dict):
        self._results.append(msg)

    def flush(self, timeout: float = None):
        pass


@pytest.fixture(scope="function")
def mock_handler():
    return MockHandler()


@pytest.fixture(scope="function")
def producer_test_url():
    return "https://atesturlthatdoesnotexist.com/page"


@pytest.fixture(scope="function")
def monitor_init_data(mock_handler, producer_test_url):
    return dict(
        url=producer_test_url,
        regex=None,
        handler=mock_handler,
    )


class TestMonitor:
    _MOCK_BODY = "something does not add up"

    @freeze_time(datetime.datetime.now())
    @pytest.mark.parametrize("status,regex,result",
                             [
                                 (200, None, "ok"),
                                 (200, "somethings?", "ok"),
                                 (400, None, "error"),
                                 (400, "somethings?", "error"),
                             ])
    def test_check(self, monitor_init_data, producer_test_url, mock_handler, status, regex, result):
        with responses.RequestsMock() as rsps:
            rsps.add(responses.GET, url=producer_test_url, body=self._MOCK_BODY, status=status)
            monitor = WebMonitor(**{**monitor_init_data, "regex": regex})
            monitor.check()
            assert len(mock_handler._results) == 1
            assert mock_handler._results[0]["url"] == producer_test_url
            assert mock_handler._results[0]["result"] == result
            assert mock_handler._results[0]["status_code"] == status
            response_time = datetime.datetime.fromisoformat(mock_handler._results[0]["response_time"])
            assert response_time == datetime.datetime.now(tz=datetime.timezone.utc)
            if regex is not None:
                assert mock_handler._results[0]["regex_matches"][regex] is True

    def test_check_failure(self, monitor_init_data, producer_test_url, mock_handler):
        with responses.RequestsMock() as rsps:
            rsps.add(responses.GET, url=producer_test_url, body=ConnectionError())
            monitor = WebMonitor(**monitor_init_data)
            monitor.check()
            assert len(mock_handler._results) == 1
            assert mock_handler._results[0]["url"] == producer_test_url
            assert mock_handler._results[0]["result"] == "failure"
