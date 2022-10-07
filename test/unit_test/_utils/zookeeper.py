from smoothcrawler_cluster._utils.zookeeper import ZookeeperNode
import pytest

from ..._values import Test_Zookeeper_Path, Test_Zookeeper_String_Value


class TestZookeeperPath:

    @pytest.fixture(scope="function")
    def zk_path(self) -> ZookeeperNode:
        return ZookeeperNode()

    def test_path(self, zk_path: ZookeeperNode):
        assert zk_path.path is None
        zk_path.path = Test_Zookeeper_Path
        assert zk_path.path == Test_Zookeeper_Path

    def test_value(self, zk_path: ZookeeperNode):
        assert zk_path.value is None
        zk_path.value = Test_Zookeeper_String_Value
        assert zk_path.value == Test_Zookeeper_String_Value
