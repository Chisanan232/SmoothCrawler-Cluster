import re
from abc import ABCMeta, abstractmethod

import pytest

from smoothcrawler_cluster._utils.zookeeper import (
    BasePath,
    MetaDataPath,
    ZookeeperNode,
    ZookeeperPath,
)

from ..._assertion import ValueFormatAssertion
from ..._values import (
    Test_Zookeeper_Path,
    Test_Zookeeper_String_Value,
    _Crawler_Group_Name_Value,
    _Crawler_Name_Value,
)


class BasePathTestSpec(metaclass=ABCMeta):
    @pytest.fixture(scope="function")
    @abstractmethod
    def ut_path(self) -> BasePath:
        pass

    @property
    @abstractmethod
    def group_parent_path(self) -> str:
        pass

    @property
    @abstractmethod
    def node_parent_path(self) -> str:
        pass

    def test_property_group_state(self, ut_path: BasePath):
        # Get value by target method for testing
        path = ut_path.group_state

        # Verify values
        ValueFormatAssertion(target=path, regex=re.escape(self.group_parent_path) + r"/[\w\-_]{1,64}/state")

    def test_property_node_state(self, ut_path: BasePath):
        # Get value by target method for testing
        path = ut_path.node_state

        # Verify values
        ValueFormatAssertion(
            target=path, regex=re.escape(self.node_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/state"
        )

    def test_property_task(self, ut_path: BasePath):
        # Get value by target method for testing
        path = ut_path.task

        # Verify values
        ValueFormatAssertion(
            target=path, regex=re.escape(self.node_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/task"
        )

    def test_property_heartbeat(self, ut_path: BasePath):
        # Get value by target method for testing
        path = ut_path.heartbeat

        # Verify values
        ValueFormatAssertion(
            target=path, regex=re.escape(self.node_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/heartbeat"
        )

    @pytest.mark.parametrize("is_group", [True, False])
    def test_generate_parent_node(self, ut_path: BasePath, is_group: bool):
        if isinstance(ut_path, MetaDataPath):
            # Get value by target method for testing
            path = ut_path.generate_parent_node(parent_name=_Crawler_Name_Value, is_group=is_group)

            # Verify values
            if is_group:
                ValueFormatAssertion(
                    target=path, regex=re.escape(self.group_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}"
                )
            else:
                ValueFormatAssertion(
                    target=path, regex=re.escape(self.node_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}"
                )


class TestMetaDataPath(BasePathTestSpec):
    @pytest.fixture(scope="function")
    def ut_path(self) -> MetaDataPath:
        return MetaDataPath(name=_Crawler_Name_Value, group=_Crawler_Group_Name_Value)

    @property
    def group_parent_path(self) -> str:
        return "group"

    @property
    def node_parent_path(self) -> str:
        return "node"


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
