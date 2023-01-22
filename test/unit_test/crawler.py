from kazoo.client import KazooClient
from unittest.mock import patch, MagicMock
from smoothcrawler_cluster.crawler import ZookeeperCrawler
from smoothcrawler_cluster.model import CrawlerStateRole, GroupState
from smoothcrawler_cluster._utils import MetaDataUtil
import pytest

from .._assertion import ValueFormatAssertion
from .._values import _Runner_Crawler_Value, _Backup_Crawler_Value


class TestZookeeperCrawler:

    @pytest.fixture(scope="function")
    def zk_crawler(self) -> ZookeeperCrawler:
        with patch.object(KazooClient, "start", return_value=None) as mock_zk_cli:
            zk_crawler = ZookeeperCrawler(runner=_Runner_Crawler_Value,
                                           backup=_Backup_Crawler_Value,
                                           initial=False,
                                           zk_hosts="1.1.1.1:8080")
            mock_zk_cli.assert_called_once()
        return zk_crawler

    def test_property_name(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing (with default, doesn't modify it by the initial options)
        crawler_name = zk_crawler.name

        # Verify values
        ValueFormatAssertion(target=crawler_name,
                             regex=r"sc-crawler_[0-9]{1,3}")

    def test_property_group(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing (with default, doesn't modify it by the initial options)
        group_name = zk_crawler.group

        # Verify values
        ValueFormatAssertion(target=group_name,
                             regex=r"sc-crawler-cluster")

    def test_property_zookeeper_hosts(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing (with default, doesn't modify it by the initial options)
        zookeeper_hosts = zk_crawler.zookeeper_hosts

        # Verify values
        ValueFormatAssertion(target=zookeeper_hosts,
                             regex="(localhost|[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}):[0-9]{1,6}")

    def test_property_group_state_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        path = zk_crawler.group_state_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=path,
                             regex=r"smoothcrawler/group/[\w\-_]{1,64}/state")

    def test_property_node_state_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        path = zk_crawler.node_state_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=path,
                             regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/state")

    def test_property_task_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        path = zk_crawler.task_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=path,
                             regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/task")

    def test_property_heartbeat_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        path = zk_crawler.heartbeat_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=path,
                             regex=r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/heartbeat")

    def test_property_ensure_register(self, zk_crawler: ZookeeperCrawler):
        # Test getter
        ensure_register = zk_crawler.ensure_register
        assert ensure_register is not None, \
            "After instantiate ZookeeperCrawler, its property 'ensure_register' should NOT be None."

        # Test setter
        zk_crawler.ensure_register = True
        ensure_register = zk_crawler.ensure_register
        assert ensure_register is True, "Property 'ensure_register' should be True as it assigning."

    def test_property_ensure_timeout(self, zk_crawler: ZookeeperCrawler):
        # Test getter
        ensure_register = zk_crawler.ensure_timeout
        assert ensure_register is not None, \
            "After instantiate ZookeeperCrawler, its property 'ensure_timeout' should NOT be None."

        # Test setter
        zk_crawler.ensure_timeout = 2
        ensure_register = zk_crawler.ensure_timeout
        assert ensure_register == 2, "Property 'ensure_timeout' should be True as it assigning."

    def test_property_ensure_wait(self, zk_crawler: ZookeeperCrawler):
        # Test getter
        ensure_register = zk_crawler.ensure_wait
        assert ensure_register is not None, \
            "After instantiate ZookeeperCrawler, its property 'ensure_wait' should NOT be None."

        # Test setter
        zk_crawler.ensure_wait = 2
        ensure_register = zk_crawler.ensure_wait
        assert ensure_register == 2, "Property 'ensure_wait' should be True as it assigning."

    def test_run_as_role_Runner(self, zk_crawler: ZookeeperCrawler):
        zk_crawler.wait_for_task = MagicMock(return_value=None)
        zk_crawler.wait_and_standby = MagicMock(return_value=None)
        zk_crawler.wait_for_to_be_standby = MagicMock(return_value=None)

        with patch.object(MetaDataUtil, "get_metadata_from_zookeeper", return_value=GroupState()) as metadata_util:
            zk_crawler.running_as_role(role=CrawlerStateRole.RUNNER)
            metadata_util.assert_not_called()
            zk_crawler.wait_for_task.assert_called_with(wait_time=2)
            zk_crawler.wait_and_standby.assert_not_called()
            zk_crawler.wait_for_to_be_standby.assert_not_called()

    def test_run_as_role_Backup_Runner(self, zk_crawler: ZookeeperCrawler):
        zk_crawler.wait_for_task = MagicMock(return_value=None)
        zk_crawler.wait_and_standby = MagicMock(return_value=None)
        zk_crawler.wait_for_to_be_standby = MagicMock(return_value=None)

        group_state = GroupState()
        group_state.standby_id = "1"
        with patch.object(MetaDataUtil, "get_metadata_from_zookeeper", return_value=group_state) as metadata_util:
            zk_crawler.running_as_role(role=CrawlerStateRole.BACKUP_RUNNER)
            metadata_util.assert_called_once()
            zk_crawler.wait_for_task.assert_not_called()
            zk_crawler.wait_and_standby.assert_called_with(wait_time=0.5, reset_timeout_threshold=10)
            zk_crawler.wait_for_to_be_standby.assert_not_called()

    def test_run_as_role_not_primary_Backup_Runner(self, zk_crawler: ZookeeperCrawler):
        zk_crawler.wait_for_task = MagicMock(return_value=None)
        zk_crawler.wait_and_standby = MagicMock(return_value=None)
        zk_crawler.wait_for_to_be_standby = MagicMock(return_value=None)

        group_state = GroupState()
        group_state.standby_id = "0"
        with patch.object(MetaDataUtil, "get_metadata_from_zookeeper", return_value=group_state) as metadata_util:
            zk_crawler.running_as_role(role=CrawlerStateRole.BACKUP_RUNNER)
            metadata_util.assert_called_once()
            zk_crawler.wait_for_task.assert_not_called()
            zk_crawler.wait_and_standby.assert_not_called()
            zk_crawler.wait_for_to_be_standby.assert_called_with(wait_time=2)

    def test_before_dead(self, zk_crawler: ZookeeperCrawler):
        try:
            zk_crawler.before_dead(Exception("Test exception"))
        except Exception as e:
            assert "Test exception" in str(e), "Its error message should be same as 'Test exception'."
        else:
            assert False, "It should raise the exception."
