from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError, NoNodeError
from smoothcrawler_cluster._utils.zookeeper import (
    _BaseZookeeperNode,
    _BaseZookeeperClient, ZookeeperClient)
from typing import TypeVar, Generic
import pytest
import random

from ..._config import Zookeeper_Hosts
from ..._values import (
    Test_Zookeeper_Path, Test_Zookeeper_Not_Exist_Path, Test_Zookeeper_String_Value, Test_Zookeeper_Bytes_Value)


_BaseZookeeperPathType = TypeVar("_BaseZookeeperPathType", bound=_BaseZookeeperNode)
_BaseZookeeperClientType = TypeVar("_BaseZookeeperClientType", bound=_BaseZookeeperClient)


class TestZookeeperClient:

    _pytest_zk_client: KazooClient = None

    @pytest.fixture(scope="function", autouse=True)
    def zk_cli(self) -> Generic[_BaseZookeeperClientType]:
        self._pytest_zk_client = KazooClient(hosts=Zookeeper_Hosts)
        self._pytest_zk_client.start()

        self._remove_zk_node_path()

        return ZookeeperClient(hosts=Zookeeper_Hosts)

    def _remove_zk_node_path(self):
        if self._pytest_zk_client.exists(path=Test_Zookeeper_Path) is not None:
            # Remove the metadata of target path in Zookeeper
            self._pytest_zk_client.delete(path=Test_Zookeeper_Path)

    def test_exist_path_with_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which already exists ---> function should return True.
        zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert zk_cli.exist_node(path=Test_Zookeeper_Path) is not None, "It should exist the path (node) it created."

    def test_exist_path_with_not_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should return False.
        assert zk_cli.exist_node(path=Test_Zookeeper_Not_Exist_Path) is None, "It should NOT exist the path (node)."

    def test_get_path_with_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which already exists ---> function should return a object which is _BaseZookeeperClientType
        # type.
        creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert creating_result is not None, "It should create a path (Zookeeper node) successfully."

        zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert zk_path.path == Test_Zookeeper_Path, \
            f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert zk_path.value == Test_Zookeeper_String_Value, \
            f"The value of *ZookeeperPath* should be equal to \"{Test_Zookeeper_String_Value}\"."

    def test_get_path_with_not_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should
        try:
            zk_cli.get_node(path=Test_Zookeeper_Not_Exist_Path)
        except NoNodeError:
            assert True, "Work finely."
        else:
            assert False, "It should raise an exception 'NoNodeError'."

    def test_create_path_without_value(self, zk_cli: Generic[_BaseZookeeperClientType]):
        creating_result = zk_cli.create_node(path=Test_Zookeeper_Path)
        assert creating_result is not None, "It should create a path (Zookeeper node) successfully."

        zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert zk_path.path == Test_Zookeeper_Path, \
            f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert zk_path.value == "", \
            "The value of *ZookeeperPath* should be equal to empty string because it created it without any value."

    def test_create_path_with_str_value(self, zk_cli: Generic[_BaseZookeeperClientType]):
        creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert creating_result is not None, "It should create a path (Zookeeper node) successfully."

        zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert zk_path.path == Test_Zookeeper_Path, \
            f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert zk_path.value == Test_Zookeeper_String_Value, \
            f"The value of *ZookeeperPath* should be equal to {Test_Zookeeper_String_Value}."

    def test_create_path_with_bytes_value(self, zk_cli: Generic[_BaseZookeeperClientType]):
        creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        assert creating_result is not None, "It should create a path (Zookeeper node) successfully."

        zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert zk_path.path == Test_Zookeeper_Path, \
            f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert zk_path.value == Test_Zookeeper_String_Value, \
            f"The value of *ZookeeperPath* should be equal to {Test_Zookeeper_String_Value}."

    def test_create_path_with_already_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        assert creating_result is not None, "It should create a path (Zookeeper node) successfully."

        try:
            zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        except NodeExistsError:
            assert True, "It works finely!"
        else:
            assert False, \
                "It should raise an exception about 'NodeExistsError' because it re-creates the existing path."

    def test_create_path_with_invalid_type_value(self, zk_cli: Generic[_BaseZookeeperClientType]):
        for _ in range(3):
            random_value = random.choice([
                ["this is test list"],
                ("this is test tuple",),
                {"this is test set"},
                {"key": "this is test dict"},
            ])

            try:
                zk_cli.create_node(path=Test_Zookeeper_Path, value=random_value)
            except TypeError:
                assert True, "It works finely."
            else:
                assert False, \
                    "It should raise an exception about 'TypeError' because the data type of function argument " \
                    "*value* is invalid."

    def test_get_value_from_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert creating_result is not None, "It should create a path (Zookeeper node) successfully."

        value = zk_cli.get_value_from_node(path=Test_Zookeeper_Path)
        assert value == Test_Zookeeper_String_Value, \
            f"The value of *ZookeeperPath* should be equal to {Test_Zookeeper_String_Value}."

    def test_set_value_to_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert creating_result is not None, "It should create a path (Zookeeper node) successfully."

        new_value = "new zookeeper value"
        zk_cli.set_value_to_node(path=Test_Zookeeper_Path, value=new_value)
        assert zk_cli.get_node(path=Test_Zookeeper_Path).value == new_value, \
            f"The new value of *ZookeeperPath* should be equal to {new_value} (by function *get_path*) because it has" \
            f" been modified."
        assert zk_cli.get_value_from_node(path=Test_Zookeeper_Path) == new_value, \
            f"The new value of *ZookeeperPath* should be equal to {new_value} (by function *get_value_from_path*) " \
            f"because it has been modified."

    def test_remove(self, zk_cli: Generic[_BaseZookeeperClientType]):
        creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert creating_result is not None, "It should create a path (Zookeeper node) successfully."
        zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert zk_path.path == Test_Zookeeper_Path, \
            f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert zk_path.value == Test_Zookeeper_String_Value, \
            f"The value of *ZookeeperPath* should be equal to {Test_Zookeeper_String_Value}."

        zk_cli.delete_node(path=Test_Zookeeper_Path)

        try:
            zk_cli.get_node(path=Test_Zookeeper_Path)
        except NoNodeError:
            assert True, "Work finely."
        else:
            assert False, "It should raise an exception 'NoNodeError'."

