from smoothcrawler_cluster._utils.zookeeper import (
    _BaseZookeeperNode,
    _BaseZookeeperClient, ZookeeperClient,
    _BaseZookeeperListener
)
from kazoo.client import KazooClient
from kazoo.exceptions import NodeExistsError
from typing import Type, TypeVar, Generic
import pytest
import random

from ..._config import Zookeeper_Hosts
from ..._values import Test_Zookeeper_Path, Test_Zookeeper_Not_Exist_Path, Test_Zookeeper_String_Value, Test_Zookeeper_Bytes_Value


_BaseZookeeperPathType = TypeVar("_BaseZookeeperPathType", bound=_BaseZookeeperNode)
_BaseZookeeperClientType = TypeVar("_BaseZookeeperClientType", bound=_BaseZookeeperClient)
_BaseZookeeperListenerType = TypeVar("_BaseZookeeperListenerType", bound=_BaseZookeeperListener)


class TestZookeeperClient:

    __PyTest_ZK_Client: KazooClient = None

    @pytest.fixture(scope="function")
    def zk_cli(self) -> Generic[_BaseZookeeperClientType]:
        self.__PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
        self.__PyTest_ZK_Client.start()

        return ZookeeperClient(hosts=Zookeeper_Hosts)

    @staticmethod
    def _remove_path_finally(test_item):

        def _(self, zk_cli: Generic[_BaseZookeeperClientType]):
            try:
                test_item(self, zk_cli)
            finally:
                # Remove the metadata of target path in Zookeeper
                self.__PyTest_ZK_Client.delete(path=Test_Zookeeper_Path)

        return _

    @_remove_path_finally
    def test_exist_path_with_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which already exists ---> function should return True.
        zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert zk_cli.exist_node(path=Test_Zookeeper_Path) is not None, "It should exist the path (node) it created."

    def test_exist_path_with_not_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should return False.
        assert zk_cli.exist_node(path=Test_Zookeeper_Not_Exist_Path) is None, "It should NOT exist the path (node)."

    @_remove_path_finally
    def test_get_path_with_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which already exists ---> function should return a object which is _BaseZookeeperClientType type.
        _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert _creating_result is not None, "It should create a path (Zookeeper node) successfully."

        _zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert _zk_path.path == Test_Zookeeper_Path, f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert _zk_path.value == Test_Zookeeper_String_Value, f"The value of *ZookeeperPath* should be equal to \"{Test_Zookeeper_String_Value}\"."

    def test_get_path_with_not_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should
        _zk_path = zk_cli.get_node(path=Test_Zookeeper_Not_Exist_Path)
        assert _zk_path.path is Test_Zookeeper_Not_Exist_Path, f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Not_Exist_Path}."
        assert _zk_path.value is None, f"The value of *ZookeeperPath* should be None because it doesn't exist recently."

    @_remove_path_finally
    def test_create_path_without_value(self, zk_cli: Generic[_BaseZookeeperClientType]):
        _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path)
        assert _creating_result is not None, "It should create a path (Zookeeper node) successfully."

        _zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert _zk_path.path == Test_Zookeeper_Path, f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert _zk_path.value == "", f"The value of *ZookeeperPath* should be equal to empty string because it created it without any value."

    @_remove_path_finally
    def test_create_path_with_str_value(self, zk_cli: Generic[_BaseZookeeperClientType]):
        _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert _creating_result is not None, "It should create a path (Zookeeper node) successfully."

        _zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert _zk_path.path == Test_Zookeeper_Path, f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert _zk_path.value == Test_Zookeeper_String_Value, f"The value of *ZookeeperPath* should be equal to {Test_Zookeeper_String_Value}."

    @_remove_path_finally
    def test_create_path_with_bytes_value(self, zk_cli: Generic[_BaseZookeeperClientType]):
        _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        assert _creating_result is not None, "It should create a path (Zookeeper node) successfully."

        _zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert _zk_path.path == Test_Zookeeper_Path, f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert _zk_path.value == Test_Zookeeper_String_Value, f"The value of *ZookeeperPath* should be equal to {Test_Zookeeper_String_Value}."

    @_remove_path_finally
    def test_create_path_with_already_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        assert _creating_result is not None, "It should create a path (Zookeeper node) successfully."

        try:
            _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        except NodeExistsError:
            assert True, "It works finely!"
        else:
            assert False, "It should raise an exception about 'NodeExistsError' because it re-creates the existing path."

    def test_create_path_with_invalid_type_value(self, zk_cli: Generic[_BaseZookeeperClientType]):
        for _ in range(3):
            random_value = random.choice([
                ["this is test list"],
                ("this is test tuple",),
                {"this is test set"},
                {"key": "this is test dict"},
            ])

            try:
                _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=random_value)
            except TypeError:
                assert True, "It works finely."
            else:
                assert False, "It should raise an exception about 'TypeError' because the data type of function argument *value* is invalid."

    @_remove_path_finally
    def test_get_value_from_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert _creating_result is not None, "It should create a path (Zookeeper node) successfully."

        _value = zk_cli.get_value_from_node(path=Test_Zookeeper_Path)
        assert _value == Test_Zookeeper_String_Value, f"The value of *ZookeeperPath* should be equal to {Test_Zookeeper_String_Value}."

    @_remove_path_finally
    def test_set_value_to_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert _creating_result is not None, "It should create a path (Zookeeper node) successfully."

        _new_value = "new zookeeper value"
        zk_cli.set_value_to_node(path=Test_Zookeeper_Path, value=_new_value)
        assert zk_cli.get_node(path=Test_Zookeeper_Path).value == _new_value, f"The new value of *ZookeeperPath* should be equal to {_new_value} (by function *get_path*) because it has been modified."
        assert zk_cli.get_value_from_node(path=Test_Zookeeper_Path) == _new_value, f"The new value of *ZookeeperPath* should be equal to {_new_value} (by function *get_value_from_path*) because it has been modified."

    def test_remove(self, zk_cli: Generic[_BaseZookeeperClientType]):
        _creating_result = zk_cli.create_node(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        assert _creating_result is not None, "It should create a path (Zookeeper node) successfully."
        _zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert _zk_path.path == Test_Zookeeper_Path, f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert _zk_path.value == Test_Zookeeper_String_Value, f"The value of *ZookeeperPath* should be equal to {Test_Zookeeper_String_Value}."

        _creating_result = zk_cli.remove_node(path=Test_Zookeeper_Path)

        _zk_path = zk_cli.get_node(path=Test_Zookeeper_Path)
        assert _zk_path.path == Test_Zookeeper_Path, f"The path of *ZookeeperPath* should be equal to {Test_Zookeeper_Path}."
        assert _zk_path.value is None, f"The value of *ZookeeperPath* should be None because the path be deleted."


class TestZookeeperListener:

    @pytest.fixture(scope="function")
    def zk_listener(self) -> Type[_BaseZookeeperListener]:
        return
