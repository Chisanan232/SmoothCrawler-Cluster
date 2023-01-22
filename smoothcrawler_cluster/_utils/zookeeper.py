"""*Util functions of operating with Zookeeper*

Here are some objects for ZookeeperCrawler which won't take care meta-data objects by itself. It would let third party
application to handle them --- Zookeeper. Therefore, some util functions about doing operations with Zookeeper in this
module for that.
"""

from abc import ABCMeta, abstractmethod
from enum import Enum
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from kazoo.recipe.lock import ReadLock, WriteLock, Semaphore
from typing import Any, Union, Optional, TypeVar, Generic

from .converter import BaseConverter


BaseConverterType = TypeVar("BaseConverterType", bound=BaseConverter)


class _BaseZookeeperNode(metaclass=ABCMeta):
    """*Framework module to define some attributes of node in Zookeeper*

    A node of Zookeeper.
    """

    @property
    @abstractmethod
    def path(self) -> str:
        """:obj:`str`: Properties with both a getter and setter for the path of node in Zookeeper."""
        pass

    @path.setter
    @abstractmethod
    def path(self, val: str) -> None:
        pass

    @property
    @abstractmethod
    def value(self) -> str:
        """:obj:`str`: Properties with both a getter and setter for the value of the path. It may need to deserialize
        the data if it needs.
        """
        pass

    @value.setter
    @abstractmethod
    def value(self, val: str) -> None:
        pass


class ZookeeperNode(_BaseZookeeperNode):
    """*Zookeeper node object*

    All data be got from Zookeeper would be converted to this object in all util functions for getting value.
    """

    _path: str = None
    _value: str = None

    @property
    def path(self) -> Optional[str]:
        return self._path

    @path.setter
    def path(self, val: str) -> None:
        self._path = val

    @property
    def value(self) -> Optional[str]:
        return self._value

    @value.setter
    def value(self, val: str) -> None:
        self._value = val


_BaseZookeeperNodeType = TypeVar("_BaseZookeeperNodeType", bound=_BaseZookeeperNode)


class ZookeeperRecipe(Enum):
    """*Distributed Lock features*

    The enum value is the object naming which could be found in the module *kazoo.recipe.lock*.
    """

    READ_LOCK: str = "ReadLock"
    """The `kazoo.recipe.lock.ReadLock`_ object.

    .. _kazoo.recipe.lock.ReadLock: https://kazoo.readthedocs.io/en/latest/api/recipe/lock.html#kazoo.recipe.lock.ReadLock # pylint: disable=line-too-long
    """

    WRITE_LOCK: str = "WriteLock"
    """The `kazoo.recipe.lock.WriteLock`_ object.

    .. _kazoo.recipe.lock.WriteLock: https://kazoo.readthedocs.io/en/latest/api/recipe/lock.html#kazoo.recipe.lock.WriteLock # pylint: disable=line-too-long
    """

    SEMAPHORE: str = "Semaphore"
    """The `kazoo.recipe.lock.Semaphore`_ object.

    .. _kazoo.recipe.lock.Semaphore: https://kazoo.readthedocs.io/en/latest/api/recipe/lock.html#kazoo.recipe.lock.Semaphore # pylint: disable=line-too-long
    """


class _BaseZookeeperClient(metaclass=ABCMeta):
    """*Framework module for defining some attributes for Zookeeper client APIs*

    Here rules some necessary APIs of Zookeeper client.
    """

    @abstractmethod
    def restrict(
            self,
            path: str,
            restrict: ZookeeperRecipe,
            identifier: str,
            max_leases: Optional[int] = None,
    ) -> Union[ReadLock, WriteLock, Semaphore]:
        """Limit Zookeeper operations in concurrency scenarios by distributed lock.

        Args:
            path (str): The node path.
            restrict (ZookeeperRecipe): Which type of distributed lock to instantiate and use.
            identifier (str): The identifier of distributed lock.
            max_leases (Optional[int]): This option for distributed lock *Semaphore*. The maximum amount to leases
                available for the semaphore. It's same as the argument of `kazoo.recipe.lock.Semaphore.__init__`_.

        Returns:
            Union[ReadLock, WriteLock, Semaphore]: The distributed lock be instantiated by *kazoo.recipe.lock.ReadLock*,
                *kazoo.recipe.lock.WriteLock* or *kazoo.recipe.lock.Semaphore*.

            The return type would be effected by the arguments *restrict* and *max_leaves*. In generally, it would
            generate the mapping object by the naming. But it would try to instantiate **Semaphore** if argument
            *max_leaves* is not None. So it DOES NOT suggest that giving value to option *max_leaves* if it doesn't
            want to use **Semaphore**.

        .. note::

            The instance it returns also could be operated by Python keyword *with*.

            .. code-block:: python

                lock = <_BaseZookeeperClient type instance>.restrict(path="/test",
                                                                     restrict=ZookeeperRecipe.READ_LOCK,
                                                                     identifier="test_id")
                with lock:
                    # Do something with the lock

        # pylint: disable=line-too-long
        .. _kazoo.recipe.lock.Semaphore.__init__: https://kazoo.readthedocs.io/en/latest/api/recipe/lock.html#kazoo.recipe.lock.Semaphore.__init__
        """
        pass

    @abstractmethod
    def exist_node(self, path: str) -> bool:
        """Check whether the target node exist or not.

        Args:
            path (str): The node path.

        Returns:
            bool: True if the target path is existed, nor False.

        """
        pass

    @abstractmethod
    def get_node(self, path: str) -> Generic[_BaseZookeeperNodeType]:
        """Get one specific node by path in Zookeeper.

        Args:
            path (str): The node path.

        Returns:
            Generic[_BaseZookeeperNodeType]: It would return a _BaseZookeeperPathType type object.

        """
        pass

    @abstractmethod
    def create_node(self, path: str, value: Union[str, bytes]) -> None:
        """Create a node with the path and value in Zookeeper.

        Args:
            path (str): The node path.
            value (Union[str, bytes]): Assign value to the node by path when create it.

        Returns:
            None

        """
        pass

    @abstractmethod
    def delete_node(self, path: str) -> bool:
        """Delete the node by path in Zookeeper.

        Args:
            path (str): The node path.

        Returns:
            bool: True if it deletes the node successfully.

        """
        pass

    @abstractmethod
    def get_value_from_node(self, path: str) -> str:
        """Get the value directly from the Zookeeper path.

        Args:
            path (str): The node path.

        Returns:
            str: The value from node in Zookeeper. It must be a string type value, but it might as a specific format,
                e.g.,JSON format, so it's possible to deserialize the data if it needs.

        """
        pass

    @abstractmethod
    def set_value_to_node(self, path: str, value: str) -> bool:
        """Set a value to the one specific Zookeeper path.

        Args:
            path (str): The node path.
            value (str): Data which must be string type value.

        Returns:
            bool: True if it runs finely without any issue, nor it returns False.

        """
        pass


class ZookeeperClient(_BaseZookeeperClient):
    """*The Zookeeper client object which be implemented by Python library `kazoo`_.*

    .. _kazoo: https://github.com/python-zk/kazoo

    This object is the default usage in this package used as Zookeeper client.
    """

    def __init__(self, hosts: str):
        self.__zk_client = KazooClient(hosts=hosts)
        self.__zk_client.start()

    def restrict(
            self,
            path: str,
            restrict: ZookeeperRecipe,
            identifier: str,
            max_leases: int = None,
    ) -> Union[ReadLock, WriteLock, Semaphore]:
        restrict_obj = getattr(self.__zk_client, str(restrict.value))
        if max_leases:
            restrict = restrict_obj(path, identifier, max_leases)
        else:
            restrict = restrict_obj(path, identifier)
        return restrict

    def exist_node(self, path: str) -> Optional[Any]:
        return self.__zk_client.exists(path=path)

    def get_node(self, path: str) -> Generic[_BaseZookeeperNodeType]:
        data, state = self.__zk_client.get(path=path)    # pylint: disable=unused-variable

        zk_path = ZookeeperNode()
        zk_path.path = path
        zk_path.value = data.decode("utf-8")

        return zk_path

    def create_node(self, path: str, value: Union[str, bytes] = None) -> str:
        if not self.exist_node(path=path):
            if not value:
                return self.__zk_client.create(path=path, makepath=True, include_data=False)

            if isinstance(value, str):
                return self.__zk_client.create(path=path, value=bytes(value, "utf-8"), makepath=True, include_data=True)
            elif isinstance(value, bytes):
                return self.__zk_client.create(path=path, value=value, makepath=True, include_data=True)
            else:
                raise TypeError("It only supports *str* or *bytes* data types.")
        else:
            raise NodeExistsError

    def delete_node(self, path: str) -> bool:
        return self.__zk_client.delete(path=path)

    def get_value_from_node(self, path: str) -> str:
        zk_path = self.get_node(path=path)
        return zk_path.value

    def set_value_to_node(self, path: str, value: Union[str, bytes]) -> None:
        if isinstance(value, str):
            self.__zk_client.set(path=path, value=value.encode("utf-8"))
        elif isinstance(value, bytes):
            self.__zk_client.set(path=path, value=value)
        else:
            raise TypeError("It only supports *str* or *bytes* data types.")

    def close(self) -> None:
        self.__zk_client.close()
