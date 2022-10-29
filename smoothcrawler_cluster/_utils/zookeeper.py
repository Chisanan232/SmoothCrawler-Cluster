from abc import ABCMeta, abstractmethod
from enum import Enum
from typing import Any, Union, Optional, TypeVar, Generic
from kazoo.client import KazooClient
from kazoo.protocol.states import ZnodeStat
from kazoo.recipe.lock import ReadLock, WriteLock, Semaphore
from kazoo.exceptions import NodeExistsError

from .converter import BaseConverter


BaseConverterType = TypeVar("BaseConverterType", bound=BaseConverter)


class _BaseZookeeperNode(metaclass=ABCMeta):
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


class ZookeeperNode(_BaseZookeeperNode):

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


_BaseZookeeperNodeType = TypeVar("_BaseZookeeperNodeType", bound=_BaseZookeeperNode)


class ZookeeperRecipe(Enum):

    ReadLock = "ReadLock"
    WriteLock = "WriteLock"
    Semaphore = "Semaphore"


class _BaseZookeeperClient(metaclass=ABCMeta):

    @abstractmethod
    def exist_node(self, path: str) -> bool:
        """
        Check whether the target node exist or not.

        :param path: The path of target node.
        :return: Boolean value. It returns True if the path exists, nor False.
        """
        pass


    @abstractmethod
    def get_node(self, path: str) -> Generic[_BaseZookeeperNodeType]:
        """
        Get one specific node by path in Zookeeper.

        :param path: The path of target node.
        :return: It would return a _BaseZookeeperPathType type object.
        """

        pass


    @abstractmethod
    def create_node(self, path: str, value: Union[str, bytes]) -> None:
        """
        Create a path as the target path in Zookeeper.

        :param path: The path of target node.
        :param value:
        :return:
        """

        pass


    @abstractmethod
    def get_value_from_node(self, path: str) -> str:
        """
        Get the value directly from the Zookeeper path.

        :param path: The path of target node.
        :return: A string type value. You may need to deserialize the data if it needs.
        """

        pass


    @abstractmethod
    def set_value_to_node(self, path: str, value: str) -> bool:
        """
        Set a value to the one specific Zookeeper path.

        :param path: The path of target node.
        :param value: A string type value.
        :return: Boolean type value, it would return True if it does finely without any issue, nor it returns False.
        """

        pass



class ZookeeperClient(_BaseZookeeperClient):

    def __init__(self, hosts: str):
        self.__zk_client = KazooClient(hosts=hosts)
        self.__zk_client.start()


    def restrict(self, path: str, restrict: ZookeeperRecipe, identifier: str, max_leases: int = None) -> Union[ReadLock, WriteLock, Semaphore]:
        _restrict_obj = getattr(self.__zk_client, str(restrict.value))
        if max_leases:
            _restrict = _restrict_obj(path, identifier, max_leases)
        else:
            _restrict = _restrict_obj(path, identifier)
        return _restrict


    def exist_node(self, path: str) -> Optional[Any]:
        return self.__zk_client.exists(path=path)


    def restrict_exist_node(self, path: str, restrict: ZookeeperRecipe, identifier: str, max_leases: int = None) -> Optional[Any]:
        with self.restrict(path=path, restrict=restrict, identifier=identifier, max_leases=max_leases):
            return self.exist_node(path)


    def get_node(self, path: str) -> Generic[_BaseZookeeperNodeType]:

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
        _zk_path = ZookeeperNode()
        _zk_path.path = path
        if _data is not None and type(_data) is bytes:
            _zk_path.value = _data.decode("utf-8")
        else:
            _zk_path.value = _data
        return _zk_path


    def restrict_get_node(self, path: str, restrict: ZookeeperRecipe, identifier: str, max_leases: int = None) -> Generic[_BaseZookeeperNodeType]:
        with self.restrict(path=path, restrict=restrict, identifier=identifier, max_leases=max_leases):
            return self.get_node(path)


    def create_node(self, path: str, value: Union[str, bytes] = None) -> str:
        if self.exist_node(path=path) is None:
            if value is None:
                return self.__zk_client.create(path=path, makepath=True, include_data=False)
    
            if type(value) is str:
                return self.__zk_client.create(path=path, value=bytes(value, "utf-8"), makepath=True, include_data=True)
            elif type(value) is bytes:
                return self.__zk_client.create(path=path, value=value, makepath=True, include_data=True)
            else:
                raise TypeError("It only supports *str* or *bytes* data types.")
        else:
            raise NodeExistsError


    # def restrict_create_node(self, path: str, restrict: ZookeeperRecipe, identifier: str, value: Union[str, bytes] = None) -> str:
    #     if restrict is not ZookeeperRecipe.WriteLock:
    #         raise RuntimeError("It should NOT use except WriteLock for writing feature.")
    #     _restrict = self._generate_restrict(path=path, restrict=restrict, identifier=identifier)
    #     with _restrict:
    #         return self.create_node(path, value)


    def delete_node(self, path: str) -> bool:
        return self.__zk_client.delete(path=path)


    def restrict_delete_node(self, path: str, restrict: ZookeeperRecipe, identifier: str, max_leases: int = None) -> bool:
        with self.restrict(path=path, restrict=restrict, identifier=identifier, max_leases=max_leases):
            return self.delete_node(path)


    def get_value_from_node(self, path: str) -> str:
        _zk_path = self.get_node(path=path)
        return _zk_path.value


    def restrict_get_value_from_node(self, path: str, restrict: ZookeeperRecipe, identifier: str, max_leases: int = None) -> str:
        _zk_path = self.restrict_get_node(path=path, restrict=restrict, identifier=identifier, max_leases=max_leases)
        return _zk_path.value


    def set_value_to_node(self, path: str, value: Union[str, bytes]) -> None:
        if type(value) is str:
            self.__zk_client.set(path=path, value=value.encode("utf-8"))
        elif type(value) is bytes:
            self.__zk_client.set(path=path, value=value)
        else:
            raise TypeError("It only supports *str* or *bytes* data types.")


    def restrict_set_value_to_node(self, path: str, value: Union[str, bytes], identifier: str) -> None:
        with self.restrict(path=path, restrict=ZookeeperRecipe.WriteLock, identifier=identifier):
            self.set_value_to_node(path, value)


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
