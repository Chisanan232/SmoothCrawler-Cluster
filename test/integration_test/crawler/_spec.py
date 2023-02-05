import multiprocessing as mp
from typing import List

from kazoo.client import KazooClient

from ..._config import Zookeeper_Hosts
from ..._values import _Total_Crawler_Value
from ..._verify import Verify, VerifyMetaData
from .._test_utils._instance_value import _ZKNodePathUtils
from .._test_utils._zk_testsuite import ZK


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
