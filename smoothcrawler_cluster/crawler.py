from datetime import datetime
from typing import List, Type, TypeVar, Generic
from abc import ABCMeta
import threading
import time

from .model import Empty, Initial, Update, CrawlerStateRole, TaskResult, HeartState, GroupState, NodeState, Task, Heartbeat
from .model.metadata import _BaseMetaData
from .election import BaseElection, IndexElection, ElectionResult
from .exceptions import ZookeeperCrawlerNotReady
from ._utils.converter import BaseConverter, JsonStrConverter
from ._utils.zookeeper import ZookeeperClient, ZookeeperRecipe


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)
BaseElectionType = TypeVar("BaseElectionType", bound=BaseElection)


class BaseDistributedCrawler(metaclass=ABCMeta):
    pass


class BaseDecentralizedCrawler(BaseDistributedCrawler):
    pass


class ZookeeperCrawler(BaseDecentralizedCrawler):

    _Zookeeper_Client: ZookeeperClient = None
    __Default_Zookeeper_Hosts: str = "localhost:2181"

    __Updating_Stop_Signal: bool = False
    __Updating_Exception = None

    def __init__(self, runner: int, backup: int, name: str = "", group: str = "", index_sep: List[str] = ["-", "_"],
                 initial: bool = True, ensure_initial: bool = False, ensure_timeout: int = 3, ensure_wait: float = 0.5,
                 zk_hosts: str = None, zk_converter: Type[BaseConverter] = None, election_strategy: Generic[BaseElectionType] = None):
        super().__init__()

        self._total_crawler = runner + backup
        self._runner = runner
        self._backup = backup
        self._current_total_crawler: List[str] = None
        self._current_total_runner: List[str] = None
        self._current_total_backup: List[str] = None
        self._standby_id: str = None
        self._crawler_role: CrawlerStateRole = None
        self._index_sep = ""
        self._ensure_register = ensure_initial
        self._ensure_timeout = ensure_timeout
        self._ensure_wait = ensure_wait

        if name == "":
            name = "sc-crawler_1"
            self._index_sep = "_"
        self._crawler_name = name

        if group == "":
            group = "sc-crawler-cluster"
        self._crawler_group = group

        self._state_identifier = "sc-crawler-cluster_state"

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
                    # Checking the separating char is valid
                    try:
                        int(_crawler_name_list[-1])
                    except ValueError:
                        continue
                    else:
                        self._index_sep = _sep
                        self._crawler_index = _crawler_name_list[-1]
                        self._election_strategy.identity = self._crawler_index
                        break
        else:
            _crawler_name_list = self._crawler_name.split(sep=self._index_sep)
            self._crawler_index = _crawler_name_list[-1]
            self._election_strategy.identity = self._crawler_index

        if initial is True:
            self.register()
            self._run_updating_heartbeat_thread()
            if self.is_ready(interval=0.5, timeout=-1):
                if self.elect() is ElectionResult.Winner:
                    self._crawler_role = CrawlerStateRole.Runner
                else:
                    self._crawler_role = CrawlerStateRole.Backup_Runner
                self._update_crawler_role(self._crawler_role)
            else:
                raise TimeoutError("Timeout to wait for crawler be ready in register process.")

    @property
    def role(self) -> CrawlerStateRole:
        return self._crawler_role

    @property
    def zookeeper_hosts(self) -> str:
        return self.__Default_Zookeeper_Hosts

    @property
    def ensure_register(self) -> bool:
        return self._ensure_register

    @ensure_register.setter
    def ensure_register(self, ensure: bool) -> None:
        self._ensure_register = ensure

    @property
    def ensure_timeout(self) -> int:
        return self._ensure_timeout

    @ensure_timeout.setter
    def ensure_timeout(self, timeout: float) -> None:
        self._ensure_timeout = timeout

    @property
    def ensure_wait(self) -> float:
        return self._ensure_wait

    @ensure_wait.setter
    def ensure_wait(self, wait: int) -> None:
        self._ensure_wait = wait

    @property
    def group_state_zookeeper_path(self) -> str:
        return f"{self.__generate_path(self._crawler_group, is_group=True)}/state"

    @property
    def node_state_zookeeper_path(self) -> str:
        return f"{self.__generate_path(self._crawler_name)}/state"

    @property
    def task_zookeeper_path(self) -> str:
        return f"{self.__generate_path(self._crawler_name)}/task"

    @property
    def heartbeat_zookeeper_path(self) -> str:
        return f"{self.__generate_path(self._crawler_name)}/heartbeat"

    def __generate_path(self, crawler_name: str, is_group: bool = False) -> str:
        if is_group is True:
            return f"smoothcrawler/group/{crawler_name}"
        else:
            return f"smoothcrawler/node/{crawler_name}"

    def register(self) -> None:
        # Question: How could it name the web crawler with number?
        # Current Answer:
        # 1. Index: get data from Zookeeper first and check the value, and it set the next index of crawler and save it to Zookeeper.
        # 2. Hardware code: Use the unique hardware code or flag to record it, i.e., the MAC address of host or something ID of container.
        self._register_group_state_to_zookeeper()
        self._register_node_to_zookeeper()
        self._register_task_to_zookeeper()
        self._register_heartbeat_to_zookeeper()

    def stop_update_heartbeat(self) -> None:
        self.__Updating_Stop_Signal = True

    def is_ready(self, interval: float = 0.5, timeout: float = -1) -> bool:
        if timeout < -1:
            raise ValueError("The option *timeout* value is incorrect. Please configure more than -1, and -1 means it never timeout.")

        _start = time.time()
        while True:
            _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            if len(_state.current_crawler) == self._total_crawler:
                self._current_total_crawler = _state.current_crawler
                self._current_total_runner = _state.current_runner
                self._current_total_backup = _state.current_backup
                self._standby_id = _state.standby_id
                return True
            if timeout != -1:
                if (time.time() - _start) >= timeout:
                    return False
            time.sleep(interval)

    def elect(self) -> ElectionResult:
        _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
        return self._election_strategy.elect(candidate=self._crawler_name, member=_state.current_crawler, index_sep=self._index_sep, spot=self._runner)

    def run(self):
        # TODO: Change to be a unlimited loop to keep running?
        if self._crawler_role is CrawlerStateRole.Runner:
            self.wait_for_task()
            self.run_task()
        elif self._crawler_role is CrawlerStateRole.Backup_Runner:
            if self._crawler_name.split(self._index_sep)[-1] == self._standby_id:
                self.wait_and_standby()
            else:
                self.wait_for_to_be_standby()

    def wait_for_task(self):
        pass

    def wait_and_standby(self) -> None:
        if self._current_total_runner is None or len(self._current_total_runner) == 0:
            raise ZookeeperCrawlerNotReady

        _timeout_record = {}

        def _chk_current_runner_heartbeat(runner_name) -> bool:
            _heartbeat_path = f"{self.__generate_path(runner_name)}/heartbeat"
            _heartbeat = self._get_metadata_from_zookeeper(path=_heartbeat_path, as_obj=Heartbeat)

            _heart_rhythm_time = _heartbeat.heart_rhythm_time
            _time_format = _heartbeat.time_format
            _update_timeout = _heartbeat.update_timeout
            _heart_rhythm_timeout = _heartbeat.heart_rhythm_timeout

            _diff_datetime = datetime.now() - datetime.strptime(_heart_rhythm_time, _time_format)
            if _diff_datetime.total_seconds() > self._get_sleep_time(_update_timeout):
                # It should start to pay attention on it
                _runner_update_timeout = _timeout_record.get(runner_name, 0)
                _timeout_record[runner_name] = _runner_update_timeout + 1
                if _timeout_record[runner_name] > int(_heart_rhythm_timeout):
                    # It should mark the runner as dead and try to activate itself.
                    _task_of_dead_crawler = self.discover(node_path=_heartbeat_path, heartbeat=_heartbeat)
                    self.activate(crawler_name=runner_name, task=_task_of_dead_crawler)
                    return True
            return False

        while True:
            _current_runners_heartbeat = map(_chk_current_runner_heartbeat, self._current_total_runner)
            if True in list(_current_runners_heartbeat):
                break

            # TODO: Parameterize this value for sleep a little bit while.
            time.sleep(2)

    def wait_for_to_be_standby(self) -> bool:
        while True:
            _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            if self._crawler_name.split(self._index_sep)[-1] == _state.standby_id:
                # Start to do wait_and_standby
                return True
            # TODO: Parameterize this value for sleep a little bit while.
            time.sleep(2)

    def run_task(self):
        pass

    def discover(self, node_path: str, heartbeat: Heartbeat) -> Task:
        _node_state_path = node_path.replace("heartbeat", "state")
        _node_state = self._get_metadata_from_zookeeper(path=_node_state_path, as_obj=NodeState)
        _node_state.role = CrawlerStateRole.Dead_Runner
        self._set_metadata_to_zookeeper(path=_node_state_path, metadata=_node_state)

        _task = self._get_metadata_from_zookeeper(path=node_path.replace("heartbeat", "task"), as_obj=Task)
        heartbeat.healthy_state = HeartState.Asystole
        heartbeat.task_state = _task.task_result
        self._set_metadata_to_zookeeper(path=node_path, metadata=heartbeat)

        return _task

    def activate(self, crawler_name: str, task: Task):
        _node_state = self._get_metadata_from_zookeeper(path=self.node_state_zookeeper_path, as_obj=NodeState)
        _node_state.role = CrawlerStateRole.Runner
        self._set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_node_state)

        with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock, identifier=self._state_identifier):
            _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)

            _state.current_runner.remove(crawler_name)
            _state.current_runner.append(self._crawler_name)
            _state.current_backup.remove(self._crawler_name)
            _state.fail_crawler.append(crawler_name)
            _state.fail_runner.append(crawler_name)
            _state.standby_id = str(int(_state.standby_id) + 1)

            self._set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state)

        if task.task_result == TaskResult.Processing.value:
            # TODO: Run task content ?
            pass
        else:
            # TODO: Do something else to handle?
            pass

    def _register_group_state_to_zookeeper(self) -> None:
        for _ in range(self._ensure_timeout):
            with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock, identifier=self._state_identifier):
                if self._Zookeeper_Client.exist_node(path=self.group_state_zookeeper_path) is None:
                    _state = Initial.group_state(
                        crawler_name=self._crawler_name,
                        total_crawler=self._runner + self._backup,
                        total_runner=self._runner,
                        total_backup=self._backup
                    )
                    self._set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state, create_node=True)
                else:
                    _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
                    if _state.current_crawler is None or self._crawler_name not in _state.current_crawler:
                        _state = Update.group_state(
                            _state,
                            total_crawler=self._runner + self._backup,
                            total_runner=self._runner,
                            total_backup=self._backup,
                            append_current_crawler=[self._crawler_name],
                            standby_id="0"
                        )
                        self._set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state)

            if self._ensure_register is False:
                break

            _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            assert _state is not None, "The meta data *State* should NOT be None."
            if len(set(_state.current_crawler)) == self._total_crawler and self._crawler_name in _state.current_crawler:
                break
            if self._ensure_wait is not None:
                time.sleep(self._ensure_wait)
        else:
            raise TimeoutError(f"It gets timeout of registering meta data *State* to Zookeeper cluster '{self.zookeeper_hosts}'.")

    def _register_node_to_zookeeper(self) -> None:
        if self._Zookeeper_Client.exist_node(path=self.node_state_zookeeper_path) is None:
            _state = Initial.node_state(group=self._crawler_group)
            self._set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_state, create_node=True)

    def _register_task_to_zookeeper(self) -> None:
        if self._Zookeeper_Client.exist_node(path=self.task_zookeeper_path) is None:
            _task = Initial.task()
            self._set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_task, create_node=True)

    def _register_heartbeat_to_zookeeper(self) -> None:
        if self._Zookeeper_Client.exist_node(path=self.heartbeat_zookeeper_path) is None:
            _heartbeat = Initial.heartbeat()
            self._set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat, create_node=True)

    def _update_crawler_role(self, role: CrawlerStateRole) -> None:
        _node_state = self._get_metadata_from_zookeeper(path=self.node_state_zookeeper_path, as_obj=NodeState)
        _updated_node_state = Update.node_state(node_state=_node_state, role=role)
        self._set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_updated_node_state)

        with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock, identifier=self._state_identifier):
            _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            if role is CrawlerStateRole.Runner:
                _updated_state = Update.group_state(
                    _state,
                    append_current_runner=[self._crawler_name],
                )
            elif role is CrawlerStateRole.Backup_Runner:
                _updated_state = Update.group_state(
                    _state,
                    append_current_backup=[self._crawler_name],
                    standby_id=self._crawler_name.split(self._index_sep)[-1]
                )
            else:
                raise ValueError(f"It doesn't support {role} recently.")

            self._set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state)

    def _run_updating_heartbeat_thread(self) -> None:
        _updating_heartbeat_thread = threading.Thread(target=self._update_heartbeat)
        _updating_heartbeat_thread.daemon = True
        _updating_heartbeat_thread.start()

    def _update_heartbeat(self) -> None:
        while True:
            if self.__Updating_Stop_Signal is False:
                try:
                    # Get *Task* and *Heartbeat* info
                    _task = self._get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                    _heartbeat = self._get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)

                    # Update the values
                    _heartbeat = Update.heartbeat(_heartbeat, heart_rhythm_time=datetime.now(), healthy_state=HeartState.Healthy, task_state=_task.task_result)
                    self._set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat)

                    # Sleep ...
                    time.sleep(self._get_sleep_time(_heartbeat.update_timeout))
                except Exception as e:
                    self.__Updating_Exception = e
                    break
            else:
                _task = self._get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                _heartbeat = self._get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)
                _heartbeat = Update.heartbeat(_heartbeat, heart_rhythm_time=datetime.now(), healthy_state=HeartState.ApparentDeath, task_state=_task.task_result)
                self._set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat)
                break
        if self.__Updating_Exception is not None:
            raise self.__Updating_Exception

    def _get_metadata_from_zookeeper(self, path: str, as_obj: Type[_BaseMetaDataType]) -> Generic[_BaseMetaDataType]:
        _value = self._Zookeeper_Client.get_value_from_node(path=path)
        if ZookeeperCrawler._value_is_not_empty(_value):
            _state = self._zk_converter.deserialize_meta_data(data=_value, as_obj=as_obj)
            return _state
        else:
            if issubclass(as_obj, GroupState):
                return Empty.group_state()
            elif issubclass(as_obj, NodeState):
                return Empty.node_state()
            elif issubclass(as_obj, Task):
                return Empty.task()
            elif issubclass(as_obj, Heartbeat):
                return Empty.heartbeat()
            else:
                raise TypeError(f"It doesn't support deserialize data as type '{as_obj}' renctly.")

    @staticmethod
    def _value_is_not_empty(_value) -> bool:
        return _value is not None and _value != ""

    def _set_metadata_to_zookeeper(self, path: str, metadata: Generic[_BaseMetaDataType], create_node: bool = False) -> None:
        _metadata_str = self._zk_converter.serialize_meta_data(obj=metadata)
        if create_node is True:
            self._Zookeeper_Client.create_node(path=path, value=_metadata_str)
        else:
            self._Zookeeper_Client.set_value_to_node(path=path, value=_metadata_str)

    @classmethod
    def _get_sleep_time(cls, timer: str) -> int:
        _time = int(timer[:-1])
        _time_unit = timer[-1]
        if _time_unit == "s":
            _sleep_time = _time
        elif _time_unit == "m":
            _sleep_time = _time * 60
        elif _time_unit == "h":
            _sleep_time = _time * 60 * 60
        else:
            raise ValueError("It only supports 's' (seconds), 'm' (minutes) or 'h' (hours) setting value.")
        return _sleep_time
