from unittest.mock import MagicMock, Mock, patch

import pytest
from kazoo.client import KazooClient

from smoothcrawler_cluster.crawler.cluster import ZookeeperCrawler
from smoothcrawler_cluster.crawler.workflow import (
    BaseRoleWorkflow,
    PrimaryBackupRunnerWorkflow,
    RunnerWorkflow,
    SecondaryBackupRunnerWorkflow,
)
from smoothcrawler_cluster.exceptions import CrawlerIsDeadError
from smoothcrawler_cluster.model import CrawlerStateRole, NodeState

from ..._assertion import ValueFormatAssertion
from ..._values import _Backup_Crawler_Value, _Runner_Crawler_Value
from ...integration_test._test_utils._instance_value import _TestValue

_Testing_Value: _TestValue = _TestValue()


def _get_workflow_arguments() -> dict:
    workflow_args = {
        "crawler_name": "test_name",
        "index_sep": "test_index_sep",
        "path": Mock(),
        "get_metadata": Mock(),
        "set_metadata": Mock(),
        "opt_metadata_with_lock": Mock(),
        "crawler_process_callback": Mock(),
    }
    return workflow_args


class TestZookeeperCrawler:
    @pytest.fixture(scope="function")
    def zk_crawler(self) -> ZookeeperCrawler:
        with patch.object(KazooClient, "start", return_value=None) as mock_zk_cli:
            zk_crawler = ZookeeperCrawler(
                runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, initial=False, zk_hosts="1.1.1.1:8080"
            )
            mock_zk_cli.assert_called_once()
        return zk_crawler

    def test_property_name(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing (with default, doesn't modify it by the initial options)
        crawler_name = zk_crawler.name

        # Verify values
        ValueFormatAssertion(target=crawler_name, regex=r"sc-crawler_[0-9]{1,3}")

    def test_property_group(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing (with default, doesn't modify it by the initial options)
        group_name = zk_crawler.group

        # Verify values
        ValueFormatAssertion(target=group_name, regex=r"sc-crawler-cluster")

    def test_property_zookeeper_hosts(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing (with default, doesn't modify it by the initial options)
        zookeeper_hosts = zk_crawler.zookeeper_hosts

        # Verify values
        ValueFormatAssertion(
            target=zookeeper_hosts, regex="(localhost|[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}):[0-9]{1,6}"
        )

    def test_property_ensure_register(self, zk_crawler: ZookeeperCrawler):
        # Test getter
        ensure_register = zk_crawler.ensure_register
        assert (
            ensure_register is not None
        ), "After instantiate ZookeeperCrawler, its property 'ensure_register' should NOT be None."

        # Test setter
        zk_crawler.ensure_register = True
        ensure_register = zk_crawler.ensure_register
        assert ensure_register is True, "Property 'ensure_register' should be True as it assigning."

    def test_property_ensure_timeout(self, zk_crawler: ZookeeperCrawler):
        # Test getter
        ensure_register = zk_crawler.ensure_timeout
        assert (
            ensure_register is not None
        ), "After instantiate ZookeeperCrawler, its property 'ensure_timeout' should NOT be None."

        # Test setter
        zk_crawler.ensure_timeout = 2
        ensure_register = zk_crawler.ensure_timeout
        assert ensure_register == 2, "Property 'ensure_timeout' should be True as it assigning."

    def test_property_ensure_wait(self, zk_crawler: ZookeeperCrawler):
        # Test getter
        ensure_register = zk_crawler.ensure_wait
        assert (
            ensure_register is not None
        ), "After instantiate ZookeeperCrawler, its property 'ensure_wait' should NOT be None."

        # Test setter
        zk_crawler.ensure_wait = 2
        ensure_register = zk_crawler.ensure_wait
        assert ensure_register == 2, "Property 'ensure_wait' should be True as it assigning."

    @pytest.mark.parametrize("role", [CrawlerStateRole.RUNNER, CrawlerStateRole.BACKUP_RUNNER])
    def test_run_finely(self, zk_crawler: ZookeeperCrawler, role: CrawlerStateRole):
        # Mock functions or objects
        mock_node_state = Mock(NodeState())
        mock_node_state.role = role.value

        zk_crawler.is_ready_for_run = MagicMock(return_value=True)
        zk_crawler.pre_running = MagicMock(return_value=None)
        zk_crawler._get_metadata = MagicMock(return_value=mock_node_state)
        zk_crawler.running_as_role = MagicMock(return_value=None)
        zk_crawler.before_dead = MagicMock(return_value=None)

        # Run target function for testing
        zk_crawler.run(unlimited=False)

        # Verify running state
        zk_crawler.is_ready_for_run.assert_called_once_with(interval=0.5, timeout=-1)
        zk_crawler.pre_running.assert_called_once()
        zk_crawler._get_metadata.assert_called_once_with(
            path=_Testing_Value.node_state_zookeeper_path, as_obj=NodeState, must_has_data=False
        )
        zk_crawler.running_as_role.assert_called_with(
            role=mock_node_state.role,
            wait_task_time=2,
            standby_wait_time=0.5,
            wait_to_be_standby_time=2,
            reset_timeout_threshold=10,
        )
        zk_crawler.before_dead.assert_not_called()

    @pytest.mark.parametrize("role", [CrawlerStateRole.RUNNER, CrawlerStateRole.BACKUP_RUNNER])
    def test_run_with_exception(self, zk_crawler: ZookeeperCrawler, role: CrawlerStateRole):
        # Mock functions or objects
        mock_node_state = Mock(NodeState())
        mock_node_state.role = role.value

    @pytest.mark.parametrize("role", [CrawlerStateRole.RUNNER, CrawlerStateRole.BACKUP_RUNNER])
    def test_run_timeout(self, zk_crawler: ZookeeperCrawler, role: CrawlerStateRole):
        # Mock functions or objects
        mock_node_state = Mock(NodeState())
        mock_node_state.role = role.value

    def test_run_as_role_runner(self, zk_crawler: ZookeeperCrawler):
        wf_args = _get_workflow_arguments()
        self._test_run_as_role(zk_crawler, role=CrawlerStateRole.RUNNER, workflow=RunnerWorkflow(**wf_args))

    def test_run_as_role_primary_backup(self, zk_crawler: ZookeeperCrawler):
        wf_args = _get_workflow_arguments()
        self._test_run_as_role(
            zk_crawler, role=CrawlerStateRole.BACKUP_RUNNER, workflow=PrimaryBackupRunnerWorkflow(**wf_args)
        )

    def test_run_as_role_secondary_backup(self, zk_crawler: ZookeeperCrawler):
        wf_args = _get_workflow_arguments()
        self._test_run_as_role(
            zk_crawler, role=CrawlerStateRole.BACKUP_RUNNER, workflow=SecondaryBackupRunnerWorkflow(**wf_args)
        )

    def _test_run_as_role(self, zk_crawler: ZookeeperCrawler, role: CrawlerStateRole, workflow: BaseRoleWorkflow):
        # Mock functions
        zk_crawler._workflow_dispatcher.dispatch = MagicMock(return_value=workflow)
        zk_crawler.stop_update_heartbeat = MagicMock(return_value=None)

        with patch.object(workflow, "run", return_value=None) as runner_wf_run:
            # Run function target to test
            zk_crawler.running_as_role(role=role)

            # Verify
            zk_crawler._workflow_dispatcher.dispatch.assert_called_once_with(role=role.value)
            runner_wf_run.assert_called_once()
            zk_crawler.stop_update_heartbeat.assert_not_called()

    @pytest.mark.parametrize("role", [CrawlerStateRole.DEAD_RUNNER, CrawlerStateRole.DEAD_BACKUP_RUNNER])
    def test_run_as_role_dead_runner(self, zk_crawler: ZookeeperCrawler, role: CrawlerStateRole):
        # Mock functions
        wf_args = _get_workflow_arguments()
        workflow = SecondaryBackupRunnerWorkflow(**wf_args)

        zk_crawler._workflow_dispatcher.dispatch = MagicMock(return_value=workflow)
        zk_crawler.stop_update_heartbeat = MagicMock(return_value=None)

        with patch.object(workflow, "run", return_value=None) as runner_wf_run:
            # Run function target to test
            try:
                zk_crawler.running_as_role(role=role)
            except CrawlerIsDeadError:
                # Verify
                zk_crawler._workflow_dispatcher.dispatch.assert_called_once_with(role=role.value)
                runner_wf_run.assert_not_called()
                zk_crawler.stop_update_heartbeat.assert_called_once()

    def test_before_dead(self, zk_crawler: ZookeeperCrawler):
        try:
            zk_crawler.before_dead(Exception("Test exception"))
        except Exception as e:
            assert "Test exception" in str(e), "Its error message should be same as 'Test exception'."
        else:
            assert False, "It should raise the exception."
