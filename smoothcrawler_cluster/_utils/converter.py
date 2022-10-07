from abc import ABCMeta, abstractmethod
from typing import Any



class BaseConverter(metaclass=ABCMeta):

    @abstractmethod
    def convert(self, data: str) -> Any:
        pass


    @abstractmethod
    def serialize(self, data: Any) -> str:
        pass


    @abstractmethod
    def deserialize(self, data: str) -> Any:
        pass
