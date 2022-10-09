from abc import ABCMeta
from smoothcrawler.crawler import SimpleCrawler

from .model.metadata import CrawlerStateRole, TaskResult, State, Task, Heartbeat
from ._utils.zookeeper import ZookeeperNode, ZookeeperClient


class BaseDistributedCrawler(metaclass=ABCMeta):
    pass


class BaseDecentralizedCrawler(BaseDistributedCrawler):
    pass


class ZookeeperCrawler(BaseDecentralizedCrawler):

    _Zookeeper_Client: ZookeeperClient = None
    __Default_Zookeeper_Hosts: str = "localhost:2181"

    def __init__(self, runner: int, backup: int, zk_hosts: str = None, name: str = ""):
        self._runner = runner
        self._backup = backup

        if zk_hosts is None:
            zk_hosts = self.__Default_Zookeeper_Hosts
        self._Zookeeper_Client = ZookeeperClient(hosts=zk_hosts)

        if name == "":
            name = "crawler"
        self._crawler_name = name

    def register(self) -> None:
        _path = f"smoothcrawler/node/{self._crawler_name}"
        if self._Zookeeper_Client.exist_node(path=_path) is False:
            _state = self._initial_state()
            self._Zookeeper_Client.create_node(path=_path, value=str(_state))
        else:
            _value = self._Zookeeper_Client.get_value_from_node(path=_path)
            _state = self._update_state(value=_value)
            self._Zookeeper_Client.set_value_to_node(path=_path, value=str(_state))

    def _initial_state(self) -> State:
        _state = State()
        _state.total_runner = self._runner
        _state.total_backup = self._backup
        _state.total_crawler = self._runner + self._backup
        return _state

    def _update_state(self, value: str) -> State:
        _state = State()
        _state.total_runner = self._runner
        _state.total_backup = self._backup
        _state.total_crawler = self._runner + self._backup
        return _state

    def is_ready(self):
        pass

    def elect(self):
        pass

    def run(self):
        pass

    def wait_for_task(self):
        pass

    def wait_and_standby(self):
        pass

    def run_task(self):
        pass

    def discover(self):
        pass

    def activate(self):
        pass
