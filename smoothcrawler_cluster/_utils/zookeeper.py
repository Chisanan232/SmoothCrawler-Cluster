from abc import ABCMeta, abstractmethod
from typing import Any, Union, TypeVar, Generic

from .converter import BaseConverter


BaseConverterType = TypeVar("BaseConverterType", bound=BaseConverter)


class _BaseZookeeperPath(metaclass=ABCMeta):
    """
    The object which saving information of one specific path of Zookeeper.
    """

    @abstractmethod
    @property
    def path(self) -> str:
        """
        The path in Zookeeper.

        :return: A string type value.
        """
        pass


    @abstractmethod
    @path.setter
    def path(self, val: str) -> None:
        """
        The path in Zookeeper.

        :return: A string type value.
        """
        pass


    @abstractmethod
    @property
    def value(self) -> str:
        """
        The value of the path.

        :return: A string type value. You may need to deserialize the data if it needs.
        """
        pass


    @abstractmethod
    @value.setter
    def value(self, val: str) -> None:
        """
        The value of the path.

        :return: A string type value. You may need to deserialize the data if it needs.
        """
        pass


# _BaseZookeeperPathType = Type["_BaseZookeeperPath", _BaseZookeeperPath]
_BaseZookeeperPathType = TypeVar("_BaseZookeeperPathType", bound=_BaseZookeeperPath)


class _BaseZookeeperClient(metaclass=ABCMeta):

    @abstractmethod
    def exist_path(self, path: str) -> bool:
        """
        Check whether the target path exist or not.

        :param path: Target path.
        :return: Boolean value. It returns True if the path exists, nor False.
        """
        pass


    @abstractmethod
    def get_path(self, path: str) -> Generic[_BaseZookeeperPathType]:
        """
        Get one specific path in Zookeeper.

        :param path: Target path.
        :return: It would return a _BaseZookeeperPathType type object.
        """

        pass


    @abstractmethod
    def create_path(self, path: str, value: Union[str, bytes]) -> None:
        """
        Create a path as the target path in Zookeeper.

        :param path:
        :param value:
        :return:
        """

        pass


    @abstractmethod
    def get_value_from_path(self, path: str) -> str:
        """
        Get the value directly from the Zookeeper path.

        :param path: Target Zookeeper path.
        :return: A string type value. You may need to deserialize the data if it needs.
        """

        pass


    @abstractmethod
    def set_value_to_path(self, path: str, value: str) -> bool:
        """
        Set a value to the one specific Zookeeper path.

        :param path: Target Zookeeper path.
        :param value: A string type value.
        :return: Boolean type value, it would return True if it does finely without any issue, nor it returns False.
        """

        pass



class _BaseZookeeperListener(metaclass=ABCMeta):

    def __init__(self, converter: BaseConverterType = None):
        self._converter = converter


    @abstractmethod
    def watch_data(self, path: str):
        pass


    @abstractmethod
    def listen_path_for_value(self, path: str, value: str) -> bool:
        pass


    @abstractmethod
    def converter(self, data: str) -> Any:
        pass
