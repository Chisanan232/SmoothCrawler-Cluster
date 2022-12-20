from smoothcrawler_cluster.crawler import ZookeeperCrawler
from kazoo.client import KazooClient
from typing import List, Dict, TypeVar, Generic, Union
from enum import Enum
from abc import ABC, abstractmethod
import pytest

from ._instance_value import _TestValue


_ZookeeperCrawlerType = TypeVar("_ZookeeperCrawlerType", bound=ZookeeperCrawler)


class ZKNode(Enum):

    GROUP_STATE = "group_state_zookeeper_path"
    NODE_STATE = "node_state_zookeeper_path"
    TASK = "task_zookeeper_path"
    HEARTBEAT = "heartbeat_zookeeper_path"


class ZK:

    _pytest_zk_client: KazooClient = None

    @staticmethod
    def reset_testing_env(path: Union[ZKNode, List[ZKNode]]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Delete target node in Zookeeper to guarantee that the runtime environment is clean.
                def _operate_zk(p):
                    if self._exist_node(path=p) is not None:
                        self._delete_node(path=p)

                self._operate_zk_before_run_testing(zk_crawler=uit_object,
                                                    path=path,
                                                    zk_function=_operate_zk,
                                                    test_item=test_item)
            return _
        return _

    @staticmethod
    def create_node_first(path: Union[ZKNode, List[ZKNode]]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Create new node in Zookeeper
                def _operate_zk(p):
                    self._create_node(path=p, include_data=False)

                self._operate_zk_before_run_testing(zk_crawler=uit_object,
                                                    path=path,
                                                    zk_function=_operate_zk,
                                                    test_item=test_item)
            return _
        return _

    @staticmethod
    def add_node_with_value_first(path_and_value: Dict[ZKNode, Union[str, bytes]]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                # Add new node with value in Zookeeper
                def _get_enum_key_from_value(p):
                    for zk_node in ZKNode:
                        inst = self._initial_zk_opt_inst(uit_object)
                        if getattr(inst, str(zk_node.value)) == p:
                            return zk_node
                    else:
                        raise ValueError(f"Cannot find the mapping enum key from the value '{p}'.")

                def _operate_zk(p):
                    key = _get_enum_key_from_value(p)
                    value = path_and_value[key]

                    if self._exist_node(path=p):
                        if isinstance(value, str):
                            self._set_value_to_node(path=p, value=bytes(value, "utf-8"))
                        elif isinstance(value, bytes):
                            self._set_value_to_node(path=p, value=value)
                        else:
                            raise TypeError("It only support type *str* and *bytes*.")
                    else:
                        if isinstance(value, str):
                            self._create_node(path=p, value=bytes(value, "utf-8"), include_data=True)
                        elif isinstance(value, bytes):
                            self._create_node(path=p, value=value, include_data=True)
                        else:
                            raise TypeError("It only support type *str* and *bytes*.")

                self._operate_zk_before_run_testing(zk_crawler=uit_object,
                                                    path=list(path_and_value.keys()),
                                                    zk_function=_operate_zk,
                                                    test_item=test_item)
            return _
        return _

    def _operate_zk_before_run_testing(
            self,
            zk_crawler: Generic[_ZookeeperCrawlerType],
            path: Union[ZKNode, List[ZKNode]],
            zk_function,
            test_item,
    ) -> None:
        paths = self._paths_to_list(path)
        for p in paths:
            inst = self._initial_zk_opt_inst(zk_crawler)
            path_str = getattr(inst, str(p.value))
            zk_function(path_str)
        test_item(self, zk_crawler)

    @staticmethod
    def remove_node_finally(path: Union[ZKNode, List[ZKNode]]):
        def _(test_item):
            def _(self, uit_object: Generic[_ZookeeperCrawlerType]):
                try:
                    test_item(self, uit_object)
                finally:
                    paths = self._paths_to_list(path)
                    for p in paths:
                        if self._exist_node(path=p.value) is not None:
                            # Remove the metadata of target path in Zookeeper
                            self._delete_node(path=p.value)
            return _
        return _

    @classmethod
    def _initial_zk_opt_inst(cls, uit_object):
        if not isinstance(uit_object, ZookeeperCrawler):
            inst = _TestValue()
        else:
            inst = uit_object
        return inst

    @classmethod
    def _paths_to_list(cls, path: Union[ZKNode, List[ZKNode]]) -> List[ZKNode]:
        if isinstance(path, list):
            return path
        elif isinstance(path, ZKNode):
            return [path]
        else:
            raise TypeError("The option *path* only accept 2 data type: *ZKNode* or *List[ZKNode]*.")

    def _exist_node(self, path: str):
        return self._pytest_zk_client.exists(path=path)

    def _create_node(self, path: str, value: bytes = None, include_data: bool = False) -> None:
        self._pytest_zk_client.create(path=path, value=value, makepath=True, include_data=include_data)

    def _set_value_to_node(self, path: str, value: bytes) -> None:
        self._pytest_zk_client.set(path=path, value=value)

    def _get_value_from_node(self, path: str) -> tuple:
        return self._pytest_zk_client.get(path=path)

    def _delete_node(self, path: str) -> None:
        self._pytest_zk_client.delete(path=path)

    def _delete_zk_nodes(self, all_paths: List[str]) -> None:
        sorted_all_paths = list(set(all_paths))
        for path in sorted_all_paths:
            if self._pytest_zk_client.exists(path=path) is not None:
                self._pytest_zk_client.delete(path=path, recursive=True)


class ZKTestSpec(ZK, ABC):

    @abstractmethod
    @pytest.fixture(scope="function")
    def uit_object(self):
        pass
