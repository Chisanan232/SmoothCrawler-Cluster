from typing import Type
from abc import ABCMeta

from .model.metadata import State, Task, Heartbeat
from ._utils.converter import BaseConverter, JsonStrConverter
from ._utils.zookeeper import ZookeeperClient


class BaseDistributedCrawler(metaclass=ABCMeta):
    pass


class BaseDecentralizedCrawler(BaseDistributedCrawler):
    pass


class ZookeeperCrawler(BaseDecentralizedCrawler):

    _Zookeeper_Client: ZookeeperClient = None
    __Default_Zookeeper_Hosts: str = "localhost:2181"

    def __init__(self, runner: int, backup: int, name: str = "", zk_hosts: str = None, zk_converter: Type[BaseConverter] = None):
        super().__init__()
        self._runner = runner
        self._backup = backup

        if name == "":
            name = "crawler"
        self._crawler_name = name

        if zk_hosts is None:
            zk_hosts = self.__Default_Zookeeper_Hosts
        self._Zookeeper_Client = ZookeeperClient(hosts=zk_hosts)

        if zk_converter is None:
            zk_converter = JsonStrConverter()
        self._zk_converter = zk_converter

    @property
    def zookeeper_hosts(self) -> str:
        return self.__Default_Zookeeper_Hosts

    def register(self) -> None:
        # Register attribute of State
        # Question: How could it name the web crawler with number?
        # Current Answer:
        # 1. Index: get data from Zookeeper first and check the value, and it set the next index of crawler and save it to Zookeeper.
        # 2. Hardware code: Use the unique hardware code or flag to record it, i.e., the MAC address of host or something ID of container.
        if self._Zookeeper_Client.exist_node(path=self.state_zookeeper_path) is False:
            _state = self._initial_state()
            self._set_state_to_zookeeper(_state, create_node=True)
        else:
            _state = self._get_state_from_zookeeper()
            _state = self._update_state(state=_state)
            self._set_state_to_zookeeper(_state)

        # Register attribute of Task
        # Register attribute of Heartbeat

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
        _state_str = str(state.to_readable_object())
        if create_node is True:
            self._Zookeeper_Client.create_node(path=self.state_zookeeper_path, value=_state_str)
        else:
            self._Zookeeper_Client.set_value_to_node(path=self.state_zookeeper_path, value=_state_str)

    def _set_task_to_zookeeper(self, task: Task, create_node: bool = False) -> None:
        _task_str = str(task.to_readable_object())
        if create_node is True:
            self._Zookeeper_Client.create_node(path=self.task_zookeeper_path, value=_task_str)
        else:
            self._Zookeeper_Client.set_value_to_node(path=self.task_zookeeper_path, value=_task_str)

    def _set_heartbeat_to_zookeeper(self, heartbeat: State, create_node: bool = False) -> None:
        _heartbeat_str = str(heartbeat.to_readable_object())
        if create_node is True:
            self._Zookeeper_Client.create_node(path=self.heartbeat_zookeeper_path, value=_heartbeat_str)
        else:
            self._Zookeeper_Client.set_value_to_node(path=self.heartbeat_zookeeper_path, value=_heartbeat_str)

    def _initial_state(self) -> State:
        _state = State()
        _state.total_runner = self._runner
        _state.total_backup = self._backup
        _state.total_crawler = self._runner + self._backup
        return _state

    def _update_state(self, state: State) -> State:
        state.total_runner = self._runner
        state.total_backup = self._backup
        state.total_crawler = self._runner + self._backup
        return state

    def _initial_task(self) -> Task:
        _task = Task()
        # TODO: Consider about the task assigning timing
        _task.task_result = None
        _task.task_content = None
        return _task

    def _update_task(self, task: Task) -> Task:
        # TODO: Consider about the task assigning timing
        task.task_result = None
        task.task_content = None
        return task

    def _initial_heartbeat(self) -> Heartbeat:
        _heartbeat = Heartbeat()
        # TODO: How to assign heartbeat value?
        import datetime
        _heartbeat.datetime = datetime.datetime.now()
        return _heartbeat

    def _update_heartbeat(self, heartbeat: Heartbeat) -> Heartbeat:
        # TODO: How to assign heartbeat value?
        import datetime
        heartbeat.datetime = datetime.datetime.now()
        return heartbeat

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
