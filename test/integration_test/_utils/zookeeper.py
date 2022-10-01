from smoothcrawler_cluster._utils.zookeeper import (
    _BaseZookeeperPath, ZookeeperPath,
    _BaseZookeeperClient, ZookeeperClient,
    _BaseZookeeperListener
)
from typing import Union, Type, TypeVar, Generic
import pytest

from ..._config import Zookeeper_Hosts
from ..._values import Test_Zookeeper_Path, Test_Zookeeper_Not_Exist_Path, Test_Zookeeper_String_Value, Test_Zookeeper_Bytes_Value


_BaseZookeeperPathType = TypeVar("_BaseZookeeperPathType", bound=_BaseZookeeperPath)
_BaseZookeeperClientType = TypeVar("_BaseZookeeperClientType", bound=_BaseZookeeperClient)
_BaseZookeeperListenerType = TypeVar("_BaseZookeeperListenerType", bound=_BaseZookeeperListener)


class TestZookeeperPath:

    @pytest.fixture(scope="function")
    def zk_path(self) -> ZookeeperPath:
        return ZookeeperPath()


    def test_path(self, zk_path: ZookeeperPath):
        assert zk_path.path is None
        zk_path.path = Test_Zookeeper_Path
        assert zk_path.path == Test_Zookeeper_Path


    def test_value(self, zk_path: ZookeeperPath):
        assert zk_path.value is None
        zk_path.value = Test_Zookeeper_String_Value
        assert zk_path.value == Test_Zookeeper_String_Value



class TestZookeeperClient:

    @pytest.fixture(scope="function")
    def zk_cli(self) -> Generic[_BaseZookeeperClientType]:
        return ZookeeperClient(hosts=Zookeeper_Hosts)


    def test_exist_path_with_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which already exists ---> function should return True.
        # zk_cli.create_path(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        zk_cli.create_path(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        assert zk_cli.exist_path(path=Test_Zookeeper_Path) is True

        # Remove the metadata of target path in Zookeeper
        zk_cli.remove(path=Test_Zookeeper_Path)
        # zk_cli.close()


    def test_exist_path_with_not_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should return False.
        assert zk_cli.exist_path(path=Test_Zookeeper_Not_Exist_Path) is None
        # zk_cli.close()


    def test_get_path_with_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which already exists ---> function should return a object which is _BaseZookeeperClientType type.
        # zk_cli.create_path(path=Test_Zookeeper_Path, value=Test_Zookeeper_String_Value)
        zk_cli.create_path(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        _zk_path = zk_cli.get_path(path=Test_Zookeeper_Path)
        assert _zk_path.path == Test_Zookeeper_Path
        assert _zk_path.value == Test_Zookeeper_String_Value

        # Remove the metadata of target path in Zookeeper
        zk_cli.remove(path=Test_Zookeeper_Path)
        # zk_cli.close()


    def test_get_path_with_not_exist_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should
        _zk_path = zk_cli.get_path(path=Test_Zookeeper_Not_Exist_Path)
        assert _zk_path.path is Test_Zookeeper_Not_Exist_Path
        assert _zk_path.value is None
        # zk_cli.close()


    def test_create_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should ...
        # Test with a path which already exists ---> function should ...
        zk_cli.create_path(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        _zk_path = zk_cli.get_path(path=Test_Zookeeper_Path)
        assert _zk_path.path == Test_Zookeeper_Path
        assert _zk_path.value == Test_Zookeeper_String_Value

        # Remove the metadata of target path in Zookeeper
        zk_cli.remove(path=Test_Zookeeper_Path)
        # zk_cli.close()


    def test_get_value_from_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should ...
        # Test with a path which already exists ---> function should ...
        zk_cli.create_path(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        _value = zk_cli.get_value_from_path(path=Test_Zookeeper_Path)
        assert _value == Test_Zookeeper_String_Value

        # Remove the metadata of target path in Zookeeper
        zk_cli.remove(path=Test_Zookeeper_Path)
        # zk_cli.close()


    def test_set_value_to_path(self, zk_cli: Generic[_BaseZookeeperClientType]):
        # Test with a path which doesn't exist ---> function should ...
        # Test with a path which already exists ---> function should ...
        zk_cli.create_path(path=Test_Zookeeper_Path, value=Test_Zookeeper_Bytes_Value)
        _new_value = "new zookeeper value"
        zk_cli.set_value_to_path(path=Test_Zookeeper_Path, value=_new_value)
        assert zk_cli.get_path(path=Test_Zookeeper_Path).value == _new_value
        assert zk_cli.get_value_from_path(path=Test_Zookeeper_Path) == _new_value

        # Remove the metadata of target path in Zookeeper
        zk_cli.remove(path=Test_Zookeeper_Path)
        # zk_cli.close()



class TestZookeeperListener:

    @pytest.fixture(scope="function")
    def zk_listener(self) -> Type[_BaseZookeeperListener]:
        return
