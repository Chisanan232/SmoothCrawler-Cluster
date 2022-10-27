from datetime import datetime
from typing import List, Type, TypeVar, Generic
from abc import ABCMeta
import time

from .model.metadata_enum import CrawlerStateRole, TaskResult
from .model.metadata import State, Task, Heartbeat
from .election import BaseElection, IndexElection, ElectionResult
from ._utils.converter import BaseConverter, JsonStrConverter
from ._utils.zookeeper import ZookeeperClient


BaseElectionType = TypeVar("BaseElectionType", bound=BaseElection)


class BaseDistributedCrawler(metaclass=ABCMeta):
    pass


class BaseDecentralizedCrawler(BaseDistributedCrawler):
    pass


class ZookeeperCrawler(BaseDecentralizedCrawler):

    _Zookeeper_Client: ZookeeperClient = None
    __Default_Zookeeper_Hosts: str = "localhost:2181"

    def __init__(self, runner: int, backup: int, name: str = "", index_sep: List[str] = ["-", "_"], initial: bool = True, zk_hosts: str = None,
                 zk_converter: Type[BaseConverter] = None, election_strategy: Generic[BaseElectionType] = None):
        super().__init__()
        self._total_crawler = runner + backup
        self._runner = runner
        self._backup = backup
        self._crawler_role: CrawlerStateRole = None
        self._index_sep = ""

        if name == "":
            name = "sc-crawler_1"
            self._index_sep = "_"
        self._crawler_name = name

        if zk_hosts is None:
            zk_hosts = self.__Default_Zookeeper_Hosts
        self._Zookeeper_Client = ZookeeperClient(hosts=zk_hosts)

        if zk_converter is None:
            zk_converter = JsonStrConverter()
        self._zk_converter = zk_converter

        if election_strategy is None:
            election_strategy = IndexElection()
        self._election_strategy = election_strategy
        if self._index_sep == "":
            for _sep in index_sep:
                _crawler_name_list = self._crawler_name.split(sep=_sep)
                if len(_crawler_name_list) > 1:
                    self._index_sep = _sep
                    self._crawler_index = _crawler_name_list[-1]
                    self._election_strategy.identity = self._crawler_index
                    break
        else:
            _crawler_name_list = self._crawler_name.split(sep=self._index_sep)
            self._crawler_index = _crawler_name_list[-1]
            self._election_strategy.identity = self._crawler_index

        if initial is True:
            # TODO: It needs create another thread to keep updating heartbeat info to signal it's alive.
            self.register()
            if self.is_ready(interval=0.5, timeout=-1):
                if self.elect() is ElectionResult.Winner:
                    self._crawler_role = CrawlerStateRole.Runner
                else:
                    self._crawler_role = CrawlerStateRole.Backup_Runner
                self._update_crawler_role(self._crawler_role)
            else:
                raise TimeoutError("")

    @property
    def role(self) -> CrawlerStateRole:
        return self._crawler_role

    @property
    def zookeeper_hosts(self) -> str:
        return self.__Default_Zookeeper_Hosts

    def register(self) -> None:
        # Register attribute of State
        # Question: How could it name the web crawler with number?
        # Current Answer:
        # 1. Index: get data from Zookeeper first and check the value, and it set the next index of crawler and save it to Zookeeper.
        # 2. Hardware code: Use the unique hardware code or flag to record it, i.e., the MAC address of host or something ID of container.
        if self._Zookeeper_Client.exist_node(path=self.state_zookeeper_path) is None:
            _state = self._initial_state()
            self._set_state_to_zookeeper(_state, create_node=True)
        else:
            _state = self._get_state_from_zookeeper()
            _state = self._update_state(state=_state)
            self._set_state_to_zookeeper(_state)

        # Register attribute of Task
        if self._Zookeeper_Client.exist_node(path=self.task_zookeeper_path) is None:
            _task = self._initial_task()
            self._set_task_to_zookeeper(_task, create_node=True)
        else:
            _task = self._get_task_from_zookeeper()
            _task = self._update_task(task=_task)
            self._set_task_to_zookeeper(_task)

        # Register attribute of Heartbeat
        if self._Zookeeper_Client.exist_node(path=self.heartbeat_zookeeper_path) is None:
            _heartbeat = self._initial_heartbeat()
            self._set_heartbeat_to_zookeeper(_heartbeat, create_node=True)
        else:
            _heartbeat = self._get_heartbeat_from_zookeeper()
            _heartbeat = self._update_heartbeat(heartbeat=_heartbeat)
            self._set_heartbeat_to_zookeeper(_heartbeat)

    @property
    def state_zookeeper_path(self) -> str:
        return f"smoothcrawler/node/{self._crawler_name}/state"

    @property
    def task_zookeeper_path(self) -> str:
        return f"smoothcrawler/node/{self._crawler_name}/task"

    @property
    def heartbeat_zookeeper_path(self) -> str:
        return f"smoothcrawler/node/{self._crawler_name}/heartbeat"

    def _get_state_from_zookeeper(self) -> State:
        _value = self._Zookeeper_Client.get_value_from_node(path=self.state_zookeeper_path)
        _state = self._zk_converter.str_to_state(data=_value)
        return _state

    def _get_task_from_zookeeper(self) -> Task:
        _value = self._Zookeeper_Client.get_value_from_node(path=self.task_zookeeper_path)
        _task = self._zk_converter.str_to_task(data=_value)
        return _task

    def _get_heartbeat_from_zookeeper(self) -> Heartbeat:
        _value = self._Zookeeper_Client.get_value_from_node(path=self.heartbeat_zookeeper_path)
        _heartbeat = self._zk_converter.str_to_heartbeat(data=_value)
        return _heartbeat

    def _set_state_to_zookeeper(self, state: State, create_node: bool = False) -> None:
        _state_str = self._zk_converter.state_to_str(state=state)
        if create_node is True:
            self._Zookeeper_Client.create_node(path=self.state_zookeeper_path, value=_state_str)
        else:
            self._Zookeeper_Client.set_value_to_node(path=self.state_zookeeper_path, value=_state_str)

    def _set_task_to_zookeeper(self, task: Task, create_node: bool = False) -> None:
        _task_str = self._zk_converter.task_to_str(task=task)
        if create_node is True:
            self._Zookeeper_Client.create_node(path=self.task_zookeeper_path, value=_task_str)
        else:
            self._Zookeeper_Client.set_value_to_node(path=self.task_zookeeper_path, value=_task_str)

    def _set_heartbeat_to_zookeeper(self, heartbeat: Heartbeat, create_node: bool = False) -> None:
        _heartbeat_str = self._zk_converter.heartbeat_to_str(heartbeat=heartbeat)
        if create_node is True:
            self._Zookeeper_Client.create_node(path=self.heartbeat_zookeeper_path, value=_heartbeat_str)
        else:
            self._Zookeeper_Client.set_value_to_node(path=self.heartbeat_zookeeper_path, value=_heartbeat_str)

    def _initial_state(self) -> State:
        _state = State()
        _state.total_crawler = self._runner + self._backup
        _state.total_runner = self._runner
        _state.total_backup = self._backup
        _state.role = CrawlerStateRole.Initial
        _state.current_crawler = [self._crawler_name]
        _state.current_runner = []
        _state.current_backup = []
        _state.standby_id = "0"
        _state.fail_crawler = []
        _state.fail_runner = []
        _state.fail_backup = []
        return _state

    def _update_state(self, state: State) -> State:
        state.total_crawler = self._runner + self._backup
        state.total_runner = self._runner
        state.total_backup = self._backup
        if state.current_crawler is None:
            state.current_crawler = []
        state.current_crawler.append(self._crawler_name)
        state.role = CrawlerStateRole.Initial
        state.standby_id = "0"
        return state

    def _update_crawler_role(self, role: CrawlerStateRole) -> None:
        _state = self._get_state_from_zookeeper()
        _state.role = role
        _state.current_crawler.append(self._crawler_name)
        if role is CrawlerStateRole.Runner:
            _state.current_runner.append(self._crawler_name)
        elif role is CrawlerStateRole.Backup_Runner:
            _state.current_backup.append(self._crawler_name)
        self._set_state_to_zookeeper(_state, create_node=False)

    def _initial_task(self) -> Task:
        _task = Task()
        # TODO: Consider about the task assigning timing
        _task.task_result = TaskResult.Nothing
        _task.task_content = {}
        return _task

    def _update_task(self, task: Task) -> Task:
        # TODO: Consider about the task assigning timing
        task.task_result = TaskResult.Nothing
        task.task_content = {}
        return task

    def _initial_heartbeat(self) -> Heartbeat:
        _heartbeat = Heartbeat()
        # TODO: How to assign heartbeat value?
        _heartbeat.datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return _heartbeat

    def _update_heartbeat(self, heartbeat: Heartbeat) -> Heartbeat:
        # TODO: How to assign heartbeat value?
        heartbeat.datetime = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return heartbeat

    def is_ready(self, interval: float = 0.5, timeout: float = -1) -> bool:
        if timeout < -1:
            raise ValueError("The option *timeout* value is incorrect. Please configure more than -1, and -1 means it never timeout.")

        _start = time.time()
        while True:
            _state = self._get_state_from_zookeeper()
            if len(set(_state.current_crawler)) == self._total_crawler:
                return True
            if timeout != -1:
                if (time.time() - _start) >= timeout:
                    return False
            time.sleep(interval)

    def elect(self) -> ElectionResult:
        _state = self._get_state_from_zookeeper()
        return self._election_strategy.elect(candidate=self._crawler_name, member=_state.current_crawler, index_sep=self._index_sep, spot=self._runner)

    def run(self):
        if self._crawler_role is CrawlerStateRole.Runner:
            self.wait_for_task()
            self.run_task()
        elif self._crawler_role is CrawlerStateRole.Backup_Runner:
            self.wait_and_standby()
            self.discover()
            self.activate()

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
