import json
from typing import Type

import pytest
from kazoo.client import KazooClient

from smoothcrawler_cluster.crawler.adapter import DistributedLock
from smoothcrawler_cluster.crawler.crawlers import ZookeeperCrawler
from smoothcrawler_cluster.crawler.dispatcher import WorkflowDispatcher
from smoothcrawler_cluster.crawler.workflow import (
    BaseRoleWorkflow,
    HeartbeatUpdatingWorkflow,
    PrimaryBackupRunnerWorkflow,
    RunnerWorkflow,
    SecondaryBackupRunnerWorkflow,
)
from smoothcrawler_cluster.exceptions import CrawlerIsDeadError
from smoothcrawler_cluster.model import CrawlerRole

from ..._config import Zookeeper_Hosts
from ..._values import _Backup_Crawler_Value, _Runner_Crawler_Value
from .._test_utils._instance_value import _TestValue
from .._test_utils._zk_testsuite import ZK, ZKNode
from ._spec import generate_crawler_name, generate_metadata_opts

_Testing_Value: _TestValue = _TestValue()


def _mock_callable(*args, **kwargs) -> None:
    pass


class TestWorkflowDispatcher(ZK):
    @pytest.fixture(scope="function")
    def uit_object(self) -> WorkflowDispatcher:
        self._pytest_zk_client = KazooClient(hosts=Zookeeper_Hosts)
        self._pytest_zk_client.start()

        zk_crawler = ZookeeperCrawler(
            name=_Testing_Value.name,
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            initial=False,
            zk_hosts=Zookeeper_Hosts,
        )

        return WorkflowDispatcher(
            name=generate_crawler_name(zk_crawler),
            path=zk_crawler._zk_path,
            metadata_opts_callback=generate_metadata_opts(zk_crawler),
            lock=DistributedLock(lock=_mock_callable),
            crawler_process_callback=_mock_callable,
        )

    def test_dispatcher_with_runner(self, uit_object: WorkflowDispatcher):
        self._test_dispatch_with_valid_roles(uit_object, CrawlerRole.RUNNER, RunnerWorkflow)

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.GROUP_STATE: _Testing_Value.group_state_data_str})
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE])
    def test_dispatcher_with_primary_backup(self, uit_object: WorkflowDispatcher):
        group_state = _Testing_Value.group_state
        group_state.standby_id = "1"
        self._set_value_to_node(
            path=_Testing_Value.group_state_zookeeper_path,
            value=bytes(json.dumps(group_state.to_readable_object()), "utf-8"),
        )
        self._test_dispatch_with_valid_roles(uit_object, CrawlerRole.BACKUP_RUNNER, PrimaryBackupRunnerWorkflow)

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.GROUP_STATE: _Testing_Value.group_state_data_str})
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE])
    def test_dispatcher_with_secondary_backup(self, uit_object: WorkflowDispatcher):
        group_state = _Testing_Value.group_state
        group_state.standby_id = "0"
        self._set_value_to_node(
            path=_Testing_Value.group_state_zookeeper_path,
            value=bytes(json.dumps(group_state.to_readable_object()), "utf-8"),
        )
        self._test_dispatch_with_valid_roles(uit_object, CrawlerRole.BACKUP_RUNNER, SecondaryBackupRunnerWorkflow)

    def _test_dispatch_with_valid_roles(
        self, uit_object: WorkflowDispatcher, role: CrawlerRole, expected_workflow: Type[BaseRoleWorkflow]
    ) -> None:
        workflow = uit_object.dispatch(role=role)
        assert workflow, "It should NOT be None."
        assert isinstance(
            workflow, expected_workflow
        ), f"It should be role **{role.value}**'s workflow {expected_workflow.__class__.__name__}."

    @pytest.mark.parametrize("role", [CrawlerRole.DEAD_RUNNER, CrawlerRole.DEAD_BACKUP_RUNNER])
    def test_dispatcher_with_dead_role(self, uit_object: WorkflowDispatcher, role: CrawlerRole):
        try:
            uit_object.dispatch(role=role)
        except CrawlerIsDeadError as e:
            expected_error_msg = (
                f"Current crawler instance '{_Testing_Value.name}' in group '{_Testing_Value.group}' is dead."
            )
            assert str(e) == expected_error_msg, f"The error message should be same as '{expected_error_msg}'."
        else:
            assert False, "It should raise an error 'CrawlerIsDeadError'."

    def test_dispatcher_with_invalid_role(self, uit_object: WorkflowDispatcher):
        under_test_role = "TestRole"
        try:
            uit_object.dispatch(role=under_test_role)
        except NotImplementedError as e:
            assert str(e) == f"It doesn't support crawler role {under_test_role} in *SmoothCrawler-Cluster*.", (
                f"Its error message should be same as 'It doesn't support crawler role {under_test_role} in "
                f"*SmoothCrawler-Cluster*.'."
            )
        else:
            assert False, "It must raises an exception if the role is an invalid role."

    def test_dispatcher_with_heartbeat_updating(self, uit_object: WorkflowDispatcher):
        workflow = uit_object.heartbeat()
        assert workflow, "It should NOT be None."
        assert isinstance(
            workflow, HeartbeatUpdatingWorkflow
        ), f"It should be workflow {HeartbeatUpdatingWorkflow.__class__.__name__}."
