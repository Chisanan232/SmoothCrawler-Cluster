import pytest

from smoothcrawler_cluster._utils.zookeeper import ZookeeperNode, ZookeeperPath

from ..._values import (
    Test_Zookeeper_Path,
    Test_Zookeeper_String_Value,
    _Crawler_Group_Name_Value,
    _Crawler_Name_Value,
)
from ..model.data import BasePathTestSpec


class TestZookeeperPath(BasePathTestSpec):
    @pytest.fixture(scope="function")
    def ut_path(self) -> ZookeeperPath:
        return ZookeeperPath(name=_Crawler_Name_Value, group=_Crawler_Group_Name_Value)

    @property
    def group_parent_path(self) -> str:
        return "smoothcrawler/group"

    @property
    def node_parent_path(self) -> str:
        return "smoothcrawler/node"


class TestZookeeperNode:
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
