from abc import ABC, abstractmethod


class Producer(ABC):
    @abstractmethod
    def send_result(self, msg: dict):
        pass

    @abstractmethod
    def flush(self, timeout: float = None):
        pass