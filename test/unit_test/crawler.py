from smoothcrawler_cluster.crawler import ZookeeperCrawler
from kazoo.client import KazooClient
from unittest.mock import patch
import pytest

from .._assertion import ValueFormatAssertion
from .._values import _Runner_Crawler_Value, _Backup_Crawler_Value


class TestZookeeperCrawler:

    @pytest.fixture(scope="function")
    def zk_crawler(self) -> ZookeeperCrawler:
        with patch.object(KazooClient, "start", return_value=None) as mock_zk_cli:
            _zk_crawler = ZookeeperCrawler(runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, initial=False, zk_hosts="1.1.1.1:8080")
            mock_zk_cli.assert_called_once()
        return _zk_crawler

    def test_property_group_state_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        _path = zk_crawler.group_state_zookeeper_path

        # Verify values
        ValueFormatAssertion(target=_path, regex=r"smoothcrawler/group/[\w\-_]{1,64}/state")

    def test_property_node_state_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        # Get value by target method for testing
        _path = zk_crawler.node_state_zookeeper_path

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
