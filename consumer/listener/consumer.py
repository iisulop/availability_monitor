from abc import abstractmethod, ABC


class Consumer(ABC):
    @abstractmethod
    def handle(self, msg: str):
        pass
