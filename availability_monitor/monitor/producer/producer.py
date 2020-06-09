from abc import ABC, abstractmethod


class Producer(ABC):
    @abstractmethod
    def send_result(self, msg: str):
        pass