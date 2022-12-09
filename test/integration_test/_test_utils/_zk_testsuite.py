from smoothcrawler_cluster.crawler import ZookeeperCrawler
from kazoo.client import KazooClient
from typing import List, Dict, TypeVar, Generic, Union
from enum import Enum
from abc import ABC, abstractmethod
import pytest

from ._instance_value import _TestValue


_ZookeeperCrawlerType = TypeVar("_ZookeeperCrawlerType", bound=ZookeeperCrawler)


class ZKNode(Enum):

    GroupState = "group_state_zookeeper_path"
    NodeState = "node_state_zookeeper_path"
    Task = "task_zookeeper_path"
    Heartbeat = "heartbeat_zookeeper_path"


class ZK:

    _PyTest_ZK_Client: KazooClient = None

    @staticmethod
    def reset_testing_env(path: Union[ZKNode, List[ZKNode]]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Delete target node in Zookeeper to guarantee that the runtime environment is clean.
                def _operate_zk(_path):
                    if self._exist_node(path=_path) is not None:
                        self._delete_node(path=_path)

                self._operate_zk_before_run_testing(zk_crawler=uit_object, path=path, zk_function=_operate_zk, test_item=test_item)
            return _
        return _

    @staticmethod
    def create_node_first(path: Union[ZKNode, List[ZKNode]]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Create new node in Zookeeper
                def _operate_zk(_path):
                    self._create_node(path=_path, include_data=False)

                self._operate_zk_before_run_testing(zk_crawler=uit_object, path=path, zk_function=_operate_zk, test_item=test_item)
            return _
        return _

    @staticmethod
    def add_node_with_value_first(path_and_value: Dict[ZKNode, Union[str, bytes]]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Add new node with value in Zookeeper
                def _get_enum_key_from_value(_path):
                    for _zk_node in ZKNode:
                        _inst = self._initial_zk_opt_inst(uit_object)
                        if getattr(_inst, str(_zk_node.value)) == _path:
                            return _zk_node
                    else:
                        raise ValueError(f"Cannot find the mapping enum key from the value '{_path}'.")

                def _operate_zk(_path):
                    _key = _get_enum_key_from_value(_path)
                    value = path_and_value[_key]

                    if self._exist_node(path=_path):
                        if type(value) is str:
                            self._set_value_to_node(path=_path, value=bytes(value, "utf-8"))
                        elif type(value) is bytes:
                            self._set_value_to_node(path=_path, value=value)
                        else:
                            raise TypeError("It only support type *str* and *bytes*.")
                    else:
                        if type(value) is str:
                            self._create_node(path=_path, value=bytes(value, "utf-8"), include_data=True)
                        elif type(value) is bytes:
                            self._create_node(path=_path, value=value, include_data=True)
                        else:
                            raise TypeError("It only support type *str* and *bytes*.")

                self._operate_zk_before_run_testing(zk_crawler=uit_object, path=list(path_and_value.keys()), zk_function=_operate_zk, test_item=test_item)
            return _
        return _

    def _operate_zk_before_run_testing(self, zk_crawler: Generic[_ZookeeperCrawlerType], path: Union[ZKNode, List[ZKNode]], zk_function, test_item):
        _paths = self._paths_to_list(path)
        for _path in _paths:
            _inst = self._initial_zk_opt_inst(zk_crawler)
            _path_str = getattr(_inst, str(_path.value))
            zk_function(_path_str)
        test_item(self, zk_crawler)

    @staticmethod
    def remove_node_finally(path: Union[ZKNode, List[ZKNode]]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                try:
                    test_item(self, uit_object)
                finally:
                    _paths = self._paths_to_list(path)
                    for _path in _paths:
                        if self._exist_node(path=_path.value) is not None:
                            # Remove the metadata of target path in Zookeeper
                            self._delete_node(path=_path.value)
            return _
        return _

    @classmethod
    def _initial_zk_opt_inst(cls, uit_object):
        if type(uit_object) is not ZookeeperCrawler:
            _inst = _TestValue()
        else:
            _inst = uit_object
        return _inst

    @classmethod
    def _paths_to_list(cls, path: Union[ZKNode, List[ZKNode]]) -> List[ZKNode]:
        if type(path) is list:
            return path
        elif type(path) is ZKNode:
            return [path]
        else:
            raise TypeError("The option *path* only accept 2 data type: *ZKNode* or *List[ZKNode]*.")

    def _exist_node(self, path: str):
        return self._PyTest_ZK_Client.exists(path=path)

    def _create_node(self, path: str, value: bytes = None, include_data: bool = False) -> None:
        self._PyTest_ZK_Client.create(path=path, value=value, makepath=True, include_data=include_data)

    def _set_value_to_node(self, path: str, value: bytes) -> None:
        self._PyTest_ZK_Client.set(path=path, value=value)

    def _get_value_from_node(self, path: str) -> tuple:
        return self._PyTest_ZK_Client.get(path=path)

    def _delete_node(self, path: str) -> None:
        self._PyTest_ZK_Client.delete(path=path)

    def _delete_zk_nodes(self, all_paths: List[str]) -> None:
        _sorted_all_paths = list(set(all_paths))
        for _path in _sorted_all_paths:
            if self._PyTest_ZK_Client.exists(path=_path) is not None:
                self._PyTest_ZK_Client.delete(path=_path, recursive=True)


class ZKTestSpec(ZK, ABC):

    @abstractmethod
    @pytest.fixture(scope="function")
    def uit_object(self):
        pass