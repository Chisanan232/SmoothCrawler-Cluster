from smoothcrawler_cluster.crawler import ZookeeperCrawler
import pytest
import re


_Runner_Value: int = 3
_Backup_Value: int = 1


class TestZookeeperCrawler:

    @pytest.fixture(scope="function")
    def zk_crawler(self) -> ZookeeperCrawler:
        return ZookeeperCrawler(runner=_Runner_Value, backup=_Backup_Value, initial=False)

    def test_property_state_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        _path = zk_crawler.state_zookeeper_path
        assert _path is not None, "The path value should not be None."
        _search_char_result = re.search(r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/state", str(_path))
        assert _search_char_result is not None, "Its format is not correct. It should be like 'smoothcrawler/node/<crawler name>/state'."

    def test_property_task_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        _path = zk_crawler.task_zookeeper_path
        assert _path is not None, "The path value should not be None."
        _search_char_result = re.search(r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/task", str(_path))
        assert _search_char_result is not None, "Its format is not correct. It should be like 'smoothcrawler/node/<crawler name>/task'."

    def test_property_heartbeat_zookeeper_path(self, zk_crawler: ZookeeperCrawler):
        _path = zk_crawler.heartbeat_zookeeper_path
        assert _path is not None, "The path value should not be None."
        _search_char_result = re.search(r"smoothcrawler/node/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/heartbeat", str(_path))
        assert _search_char_result is not None, "Its format is not correct. It should be like 'smoothcrawler/node/<crawler name>/heartbeat'."
