from abc import ABCMeta, abstractmethod
from typing import Any, Union, Optional, TypeVar, Generic
from kazoo.client import KazooClient
from kazoo.protocol.states import ZnodeStat

from .converter import BaseConverter


BaseConverterType = TypeVar("BaseConverterType", bound=BaseConverter)


class _BaseZookeeperPath(metaclass=ABCMeta):
    """
    The object which saving information of one specific path of Zookeeper.
    """

    @property
    @abstractmethod
    def path(self) -> str:
        """
        The path in Zookeeper.

        :return: A string type value.
        """
        pass


    @path.setter
    @abstractmethod
    def path(self, val: str) -> None:
        """
        The path in Zookeeper.

        :return: A string type value.
        """
        pass


    @property
    @abstractmethod
    def value(self) -> str:
        """
        The value of the path.

        :return: A string type value. You may need to deserialize the data if it needs.
        """
        pass


    @value.setter
    @abstractmethod
    def value(self, val: str) -> None:
        """
        The value of the path.

        :return: A string type value. You may need to deserialize the data if it needs.
        """
        pass


class ZookeeperPath(_BaseZookeeperPath):

    __path: str = None
    __value: str = None

    @property
    def path(self) -> Optional[str]:
        return self.__path

    @path.setter
    def path(self, val: str) -> None:
        self.__path = val

    @property
    def value(self) -> Optional[str]:
        return self.__value

    @value.setter
    def value(self, val: str) -> None:
        self.__value = val


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



class ZookeeperClient(_BaseZookeeperClient):

    def __init__(self, hosts: str):
        self.__zk_client = KazooClient(hosts=hosts)
        self.__zk_client.start()


    def exist_path(self, path: str) -> bool:
        return self.__zk_client.exists(path=path)


    def get_path(self, path: str) -> Generic[_BaseZookeeperPathType]:

        def _get_value() -> (bytes, ZnodeStat):
            __data = None
            __state = None

            @self.__zk_client.DataWatch(path)
            def _get_value_from_path(data: bytes, state: ZnodeStat):
                nonlocal __data, __state
                __data = data
                __state = state

            return __data, __state

        _data, _state = _get_value()
        _zk_path = ZookeeperPath()
        _zk_path.path = path
        if _data is not None and type(_data) is bytes:
            _zk_path.value = _data.decode("utf-8")
        return _zk_path


    def create_path(self, path: str, value: Union[str, bytes]) -> None:
        if type(value) is str:
            self.__zk_client.create(path=path, value=bytes(value, "utf-8"))
        elif type(value) is bytes:
            self.__zk_client.create(path=path, value=value)
        else:
            raise TypeError("It only supports *str* or *bytes* data types.")


    def remove(self, path: str) -> bool:
        return self.__zk_client.delete(path=path)


    def get_value_from_path(self, path: str) -> str:
        _zk_path = self.get_path(path=path)
        return _zk_path.value


    def set_value_to_path(self, path: str, value: str) -> None:
        self.__zk_client.set(path=path, value=value)


    def close(self) -> None:
        self.__zk_client.close()



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
