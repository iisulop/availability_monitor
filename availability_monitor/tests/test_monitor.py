import pytest
import responses

from monitor.monitor import Monitor
from monitor.producer import Producer


class MockProducer(Producer):
    def __init__(self):
        self._results = []

    def send_result(self, msg: dict):
        self._results.append(msg)

    def flush(self, timeout: float = None):
        pass


@pytest.fixture(scope="function")
def mock_producer():
    return MockProducer()


@pytest.fixture(scope="function")
def producer_test_url():
    return "https://atesturlthatdoesnotexist.com/page"


@pytest.fixture(scope="function")
def monitor_init_data(mock_producer, producer_test_url):
    return dict(
        url=producer_test_url,
        regex=None,
        producer=mock_producer,
    )


class TestMonitor:
    _MOCK_BODY = "something does not add up"

    @pytest.mark.parametrize("status,regex,result",
                             [
                                 (200, None, "ok"),
                                 (200, "somethings?", "ok"),
                                 (400, None, "error"),
                                 (400, "somethings?", "error"),
                             ])
    def test_check(self, monitor_init_data, producer_test_url, mock_producer, status, regex, result):
        with responses.RequestsMock() as rsps:
            rsps.add(responses.GET, url=producer_test_url, body=self._MOCK_BODY, status=status)
            monitor = Monitor(**{**monitor_init_data, "regex": regex})
            monitor.check()
            assert len(mock_producer._results) == 1
            assert mock_producer._results[0]['url'] == producer_test_url
            assert mock_producer._results[0]['result'] == result
            assert mock_producer._results[0]['status_code'] == status
            assert mock_producer._results[0]['response_time'] is not None
            if regex is not None:
                assert mock_producer._results[0]['regex_matches'][regex] == True

    def test_check_failure(self, monitor_init_data, producer_test_url, mock_producer):
        with responses.RequestsMock() as rsps:
            rsps.add(responses.GET, url=producer_test_url, body=ConnectionError())
            monitor = Monitor(**monitor_init_data)
            monitor.check()
            assert len(mock_producer._results) == 1
            assert mock_producer._results[0]['url'] == producer_test_url
            assert mock_producer._results[0]['result'] == "failure"
