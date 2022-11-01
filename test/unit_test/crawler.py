from smoothcrawler_cluster.crawler import ZookeeperCrawler
from smoothcrawler_cluster.model.metadata import State, Task, Heartbeat
from smoothcrawler_cluster.model.metadata_enum import CrawlerStateRole, TaskResult
import pytest

from .._assertion import (
    WorkingTime,
    ObjectIsNoneOrNotAssertion, ValueAssertion, ListSizeAssertion, ValueFormatAssertion
)


_Runner_Value: int = 2
_Backup_Value: int = 1


class TestZookeeperCrawler:

    @pytest.fixture(scope="function")
    def zk_crawler(self) -> ZookeeperCrawler:
        return ZookeeperCrawler(runner=_Runner_Value, backup=_Backup_Value, initial=False)

    def test_property_state_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        _path = zk_crawler.state_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=_path, regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/state")

    def test_property_task_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        _path = zk_crawler.task_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=_path, regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/task")

    def test_property_heartbeat_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        _path = zk_crawler.heartbeat_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=_path, regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/heartbeat")

    def test__initial_state(self, zk_crawler: ZookeeperCrawler):
        # Operate target method for testing
        _state = zk_crawler._initial_state()

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _state, is_none=False)

        ValueAssertion(WorkingTime.AtInitial, _state, metadata="role", expected_value=CrawlerStateRole.Initial.value)

        ValueAssertion(WorkingTime.AtInitial, _state, metadata="total_crawler", expected_value=_Runner_Value + _Backup_Value)
        ValueAssertion(WorkingTime.AtInitial, _state, metadata="total_runner", expected_value=_Runner_Value)
        ValueAssertion(WorkingTime.AtInitial, _state, metadata="total_backup", expected_value=_Backup_Value)

        ValueAssertion(WorkingTime.AtInitial, _state, metadata="standby_id", expected_value="0")

        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_crawler", expected_value=1)
        assert _state.current_crawler[0] == zk_crawler._crawler_name, \
            f"In initialing process, meta data *state.current_crawler* should save value '{zk_crawler._crawler_name}'."
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_backup", expected_value=0)

        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_crawler", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_backup", expected_value=0)

    def test__update_state(self, zk_crawler: ZookeeperCrawler):
        # Operate target method for testing
        _state = State()
        _updated_state = zk_crawler._update_state(_state)

        # Verify values
        ValueAssertion(WorkingTime.AfterUpdateInInitial, _updated_state, metadata="role", expected_value=CrawlerStateRole.Initial.value)

        ValueAssertion(WorkingTime.AfterUpdateInInitial, _updated_state, metadata="total_crawler", expected_value=_Runner_Value + _Backup_Value)
        ValueAssertion(WorkingTime.AfterUpdateInInitial, _updated_state, metadata="total_runner", expected_value=_Runner_Value)
        ValueAssertion(WorkingTime.AfterUpdateInInitial, _updated_state, metadata="total_backup", expected_value=_Backup_Value)

        ValueAssertion(WorkingTime.AfterUpdateInInitial, _updated_state, metadata="standby_id", expected_value="0")

        ListSizeAssertion(WorkingTime.AfterUpdateInInitial, _updated_state, metadata="current_crawler", expected_value=1)
        assert _updated_state.current_crawler[0] == zk_crawler._crawler_name, \
            f"In initialing process, meta data *state.current_crawler* should save value '{zk_crawler._crawler_name}'."

    def test__initial_task(self, zk_crawler: ZookeeperCrawler):
        # Operate target method for testing
        _task = zk_crawler._initial_task()

        # Verify values
        ValueAssertion(WorkingTime.AtInitial, _task, metadata="task_result", expected_value=TaskResult.Nothing.value)
        ValueAssertion(WorkingTime.AtInitial, _task, metadata="task_content", expected_value={})

    def test__update_task(self, zk_crawler: ZookeeperCrawler):
        # Operate target method for testing
        _task = Task()
        _updated_task = zk_crawler._update_task(_task)

        # Verify values
        ValueAssertion(WorkingTime.AfterUpdateInInitial, _updated_task, metadata="task_result", expected_value=TaskResult.Nothing.value)
        ValueAssertion(WorkingTime.AfterUpdateInInitial, _updated_task, metadata="task_content", expected_value={})

    def test__initial_heartbeat(self, zk_crawler: ZookeeperCrawler):
        # Operate target method for testing
        _heartbeat = zk_crawler._initial_heartbeat()

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _heartbeat, is_none=False)

    def test__update_heartbeat(self, zk_crawler: ZookeeperCrawler):
        # Operate target method for testing
        _heartbeat = Heartbeat()
        _updated_heartbeat = zk_crawler._update_heartbeat(_heartbeat)

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AfterUpdateInInitial, _updated_heartbeat, is_none=False)
