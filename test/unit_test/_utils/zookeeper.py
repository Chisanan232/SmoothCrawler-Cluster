import pytest

from smoothcrawler_cluster._utils.zookeeper import ZookeeperNode, ZookeeperPath

from ..._assertion import ValueFormatAssertion
from ..._values import (
    Test_Zookeeper_Path,
    Test_Zookeeper_String_Value,
    _Crawler_Group_Name_Value,
    _Crawler_Name_Value,
)


class TestZookeeperPath:
    @pytest.fixture(scope="function")
    def zk_path(self) -> ZookeeperPath:
        return ZookeeperPath(name=_Crawler_Name_Value, group=_Crawler_Group_Name_Value)

    def test_property_group_state_zookeeper_path(self, zk_path: ZookeeperPath):
        # Get value by target method for testing
        path = zk_path.group_state_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=path, regex=r"smoothcrawler/group/[\w\-_]{1,64}/state")

    def test_property_node_state_zookeeper_path(self, zk_path: ZookeeperPath):
        # Get value by target method for testing
        path = zk_path.node_state_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=path, regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/state")

    def test_property_task_zookeeper_path(self, zk_path: ZookeeperPath):
        # Get value by target method for testing
        path = zk_path.task_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=path, regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/task")

    def test_property_heartbeat_zookeeper_path(self, zk_path: ZookeeperPath):
        # Get value by target method for testing
        path = zk_path.heartbeat_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=path, regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/heartbeat")


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
