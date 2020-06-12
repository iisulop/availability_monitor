from abc import ABC, abstractmethod
from typing import Optional, Any, Dict


class Handler(ABC):
    """
    Class for handling messages passed to cobjects implementing this class via `send_msg`.
    """
    @abstractmethod
    def send_msg(self, message: Dict[Any, Any]):
        """
        Send the given `message` forward

        :param message: `dict` of information from WebMonitor
        """

    @abstractmethod
    def flush(self, timeout: Optional[float] = None):
        """
        Force send any queued messages.

        :param timeout: Optional timeout in seconds to wait before giving up
        """
