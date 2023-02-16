import multiprocessing as mp
from typing import List

from kazoo.client import KazooClient

from smoothcrawler_cluster.crawler import ZookeeperCrawler
from smoothcrawler_cluster.model._data import CrawlerName, MetaDataOpt

from ..._config import Zookeeper_Hosts
from ..._values import _Total_Crawler_Value
from ..._verify import Verify, VerifyMetaData
from .._test_utils._instance_value import _ZKNodePathUtils
from .._test_utils._zk_testsuite import ZK


def generate_crawler_name(zk_crawler: ZookeeperCrawler = None) -> CrawlerName:
    name = CrawlerName()
    if zk_crawler:
        name.group = zk_crawler.group
        name.base_name = zk_crawler.name.split(zk_crawler._index_sep)[0]
        name.index_separation = zk_crawler._index_sep
        name.id = zk_crawler.name.split(zk_crawler._index_sep)[-1]
    else:
        name.group = "pytest"
        name.base_name = "sc-crawler"
        name.index_separation = "_"
        name.id = "1"
    return name


def generate_metadata_opts(zk_crawler: ZookeeperCrawler = None) -> MetaDataOpt:
    metadata_opts = MetaDataOpt()
    metadata_opts.exist_callback = zk_crawler._exist_metadata
    metadata_opts.get_callback = zk_crawler._get_metadata
    metadata_opts.set_callback = zk_crawler._set_metadata
    return metadata_opts


class MultiCrawlerTestSuite(ZK):

    _processes: List[mp.Process] = []

    _verify = Verify()
    _verify_metadata = VerifyMetaData()

    @staticmethod
    def _clean_environment(function):
        def _(self):
            # Initial Zookeeper session
            self._pytest_zk_client = KazooClient(hosts=Zookeeper_Hosts)
            self._pytest_zk_client.start()

            self._verify_metadata.initial_zk_session(self._pytest_zk_client)

            # Reset Zookeeper nodes first
            self._reset_all_metadata(size=_Total_Crawler_Value)

            # Reset workers collection
            self._processes.clear()

            try:
                # Run the test item
                function(self)
            finally:
                # Kill all processes
                for process in self._processes:
                    if isinstance(process, mp.Process):
                        process.terminate()
                # Reset Zookeeper nodes fianlly
                self._reset_all_metadata(size=_Total_Crawler_Value)

        return _

    def _reset_all_metadata(self, size: int) -> None:
        all_paths = _ZKNodePathUtils.all(size)
        self._delete_zk_nodes(all_paths)
