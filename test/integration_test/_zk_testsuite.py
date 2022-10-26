from smoothcrawler_cluster.crawler import ZookeeperCrawler
from kazoo.client import KazooClient
from typing import TypeVar, Generic, Union
from abc import ABC, abstractmethod
import pytest


_ZookeeperCrawlerType = TypeVar("_ZookeeperCrawlerType", bound=ZookeeperCrawler)


class ZK:

    _PyTest_ZK_Client: KazooClient = None

    @staticmethod
    def reset_testing_env(path: str):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Delete target node in Zookeeper to guarantee that the runtime environment is clean.
                if self._PyTest_ZK_Client.exists(path=path) is not None:
                    self._PyTest_ZK_Client.delete(path=path)
                test_item(self, uit_object)
            return _
        return _

    @staticmethod
    def create_node_first(path: str):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Create new node in Zookeeper
                self._PyTest_ZK_Client.create(path=path, makepath=True, include_data=False)
                test_item(self, uit_object)
            return _
        return _

    @staticmethod
    def add_node_with_value_first(path: str, value: Union[str, bytes]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Add new node with value in Zookeeper
                if self._PyTest_ZK_Client.exists(path=path):
                    if type(value) is str:
                        self._PyTest_ZK_Client.set(path=path, value=value)
                    elif type(value) is bytes:
                        self._PyTest_ZK_Client.set(path=path, value=value.encode("utf-8"))
                    else:
                        raise TypeError("It only support type *str* and *bytes*.")
                else:
                    if type(value) is str:
                        self._PyTest_ZK_Client.create(path=path, value=bytes(value, "utf-8"), makepath=True, include_data=True)
                    elif type(value) is bytes:
                        self._PyTest_ZK_Client.create(path=path, value=value, makepath=True, include_data=True)
                    else:
                        raise TypeError("It only support type *str* and *bytes*.")

                test_item(self, uit_object)
            return _
        return _

    @staticmethod
    def remove_node_finally(path: str):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                try:
                    test_item(self, uit_object)
                finally:
                    if self._PyTest_ZK_Client.exists(path=path) is not None:
                        # Remove the metadata of target path in Zookeeper
                        self._PyTest_ZK_Client.delete(path=path)
            return _
        return _


class ZKTestSpec(ZK, ABC):

    @abstractmethod
    @pytest.fixture(scope="function")
    def uit_object(self):
        pass
