from smoothcrawler.crawler import BaseCrawler
from smoothcrawler.factory import BaseFactory
from datetime import datetime
from typing import List, Any, Type, TypeVar, Union, Generic
from abc import ABCMeta
import threading
import time

from .model import (
    # Zookeeper operating common functions
    Empty, Initial, Update,
    # Enum objects
    CrawlerStateRole, TaskResult, HeartState,
    # Content data namedtuple object
    RunningContent, RunningResult, ResultDetail,
    # Meta data objects
    GroupState, NodeState, Task, Heartbeat
)
from .model.metadata import _BaseMetaData
from .election import BaseElection, IndexElection, ElectionResult
from .exceptions import ZookeeperCrawlerNotReady
from ._utils.converter import BaseConverter, JsonStrConverter, TaskContentDataUtils
from ._utils.zookeeper import ZookeeperClient, ZookeeperRecipe


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)
BaseElectionType = TypeVar("BaseElectionType", bound=BaseElection)


class BaseDistributedCrawler(metaclass=ABCMeta):
    pass


class BaseDecentralizedCrawler(BaseDistributedCrawler):
    pass


class ZookeeperCrawler(BaseDecentralizedCrawler, BaseCrawler):

    _Zookeeper_Client: ZookeeperClient = None
    __Default_Zookeeper_Hosts: str = "localhost:2181"

    _Zookeeper_GroupState_Node_Path: str = "state"
    _Zookeeper_NodeState_Node_Path: str = "state"
    _Zookeeper_Task_Node_Path: str = "task"
    _Zookeeper_Heartbeat_Node_Path: str = "heartbeat"

    __Updating_Stop_Signal: bool = False
    __Updating_Exception = None

    def __init__(self, runner: int, backup: int, name: str = "", group: str = "", index_sep: List[str] = ["-", "_"],
                 initial: bool = True, ensure_initial: bool = False, ensure_timeout: int = 3, ensure_wait: float = 0.5,
                 zk_hosts: str = None, zk_converter: Type[BaseConverter] = None, election_strategy: Generic[BaseElectionType] = None,
                 factory: BaseFactory = None):
        super(ZookeeperCrawler, self).__init__(factory=factory)

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
        return f"{self._generate_path(self._crawler_group, is_group=True)}/{self._Zookeeper_GroupState_Node_Path}"

    @property
    def node_state_zookeeper_path(self) -> str:
        return f"{self._generate_path(self._crawler_name)}/{self._Zookeeper_NodeState_Node_Path}"

    @property
    def task_zookeeper_path(self) -> str:
        return f"{self._generate_path(self._crawler_name)}/{self._Zookeeper_Task_Node_Path}"

    @property
    def heartbeat_zookeeper_path(self) -> str:
        return f"{self._generate_path(self._crawler_name)}/{self._Zookeeper_Heartbeat_Node_Path}"

    @classmethod
    def _generate_path(cls, crawler_name: str, is_group: bool = False) -> str:
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
                return True
            if timeout != -1:
                if (time.time() - _start) >= timeout:
                    return False
            time.sleep(interval)

    def elect(self) -> ElectionResult:
        _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
        return self._election_strategy.elect(candidate=self._crawler_name, member=_state.current_crawler, index_sep=self._index_sep, spot=self._runner)

    def run(self) -> None:
        while True:
            self.pre_running()
            try:
                self.running_as_role(self._crawler_role)
            except Exception as e:
                self.before_dead(e)
            time.sleep(1)

    def pre_running(self) -> None:
        pass

    def running_as_role(self, role: CrawlerStateRole) -> None:
        if role is CrawlerStateRole.Runner:
            print(f"[DEBUG - {self._crawler_name}::running_as_role] wait_for_task")
            self.wait_for_task()
        elif role is CrawlerStateRole.Backup_Runner:
            if self._crawler_name.split(self._index_sep)[-1] == self._standby_id:
                print(f"[DEBUG - {self._crawler_name}::running_as_role] wait_and_standby")
                self.wait_and_standby()
            else:
                print(f"[DEBUG - {self._crawler_name}::running_as_role] wait_for_to_be_standby")
                self.wait_for_to_be_standby()

    def before_dead(self, exception: Exception) -> None:
        raise exception

    def wait_for_task(self) -> None:
        # 1. Try to get data from the target mode path of Zookeeper
        # 2. if (step 1 has data and it's valid) {
        #        Start to run tasks from the data
        #    } else {
        #        Keep wait for tasks
        #    }
        while True:
            _task = self._get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task, must_has_data=False)
            print(f"[DEBUG - {self._crawler_name}::wait_for_task] task: {_task}.")
            if _task.running_content is not None and len(_task.running_content) >= 1:
                # Start to run tasks ...
                print(f"[DEBUG - {self._crawler_name}::wait_for_task] run task.")
                self.run_task(task=_task)
            else:
                # Keep waiting
                # TODO: Parameterize this value for sleep a little bit while.
                time.sleep(2)

    def wait_and_standby(self) -> None:
        print(f"[DEBUG - {self._crawler_name}::wait_and_standby] start wait_and_standby.")
        _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
        print(f"[DEBUG - {self._crawler_name}::wait_and_standby] _state: {_state}.")
        if _state.current_crawler is None or len(_state.current_crawler) != self._total_crawler or \
                _state.current_runner is None or len(_state.current_runner) != self._runner or \
                _state.current_backup is None or len(_state.current_backup) != self._backup:
            print(f"[DEBUG] - {self._crawler_name} raise ZookeeperCrawlerNotReady.")
            raise ZookeeperCrawlerNotReady

        _timeout_record = {}

        def _chk_current_runner_heartbeat(runner_name: str) -> bool:
            print(f"[DEBUG - _chk_current_runner_heartbeat] runner_name: {runner_name}")
            _heartbeat_path = f"{self._generate_path(runner_name)}/{self._Zookeeper_Heartbeat_Node_Path}"
            _heartbeat = self._get_metadata_from_zookeeper(path=_heartbeat_path, as_obj=Heartbeat)

            _heart_rhythm_time = _heartbeat.heart_rhythm_time
            _time_format = _heartbeat.time_format
            _update_timeout = _heartbeat.update_timeout
            _heart_rhythm_timeout = _heartbeat.heart_rhythm_timeout

            _diff_datetime = datetime.now() - datetime.strptime(_heart_rhythm_time, _time_format)
            if _diff_datetime.total_seconds() >= self._get_sleep_time(_update_timeout):
                print(f"[DEBUG - _chk_current_runner_heartbeat] runner_name: {runner_name}, update heartbeat timeout")
                # It should start to pay attention on it
                _runner_update_timeout = _timeout_record.get(runner_name, 0)
                _timeout_record[runner_name] = _runner_update_timeout + 1
                if _timeout_record[runner_name] >= int(_heart_rhythm_timeout):
                    print(f"[DEBUG - _chk_current_runner_heartbeat] runner_name: {runner_name}, it timeout too many times to be marked as dead")
                    # It should mark the runner as dead and try to activate itself.
                    _task_of_dead_crawler = self.discover(crawler_name=runner_name, heartbeat=_heartbeat)
                    self.activate(crawler_name=runner_name, task=_task_of_dead_crawler)
                    return True
            return False

        print(f"[DEBUG - wait_and_standby] Start while loop ...")
        while True:
            _group_state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            print(f"[DEBUG - wait_and_standby] self._crawler_name: {self._crawler_name}, _state: {_group_state}")
            _current_runners_heartbeat = map(_chk_current_runner_heartbeat, _group_state.current_runner)
            if True in list(_current_runners_heartbeat):
                break

            # TODO: Parameterize this value for sleep a little bit while.
            time.sleep(0.4)

    def wait_for_to_be_standby(self) -> bool:
        while True:
            # _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            if self._crawler_name.split(self._index_sep)[-1] == self._standby_id:
                # Start to do wait_and_standby
                return True
            # TODO: Parameterize this value for sleep a little bit while.
            time.sleep(2)

    def run_task(self, task: Task) -> None:
        _running_content = task.running_content
        _current_task: Task = None
        for _index, _content in enumerate(_running_content):
            _content = TaskContentDataUtils.convert_to_running_content(_content)

            # Update the ongoing task ID
            _original_task = self._get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
            if _index == 0:
                _current_task = Update.task(task=_original_task, in_progressing_id=_content.task_id, running_status=TaskResult.Processing)
            else:
                _current_task = Update.task(task=_original_task, in_progressing_id=_content.task_id)
            self._set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_current_task)

            # Run the task and update the meta data Task
            _data = None
            try:
                # TODO: Consider of here usage about how to implement to let it be more clear and convenience in usage in cient site
                _data = self._run_crawling_processing(_content)
            except NotImplementedError as e:
                raise e
            except Exception as e:
                # Update attributes with fail result
                _running_result = TaskContentDataUtils.convert_to_running_result(_original_task.running_result)
                _updated_running_result = RunningResult(success_count=_running_result.success_count, fail_count=_running_result.fail_count + 1)

                _result_detail = _original_task.result_detail
                _result_detail.append(ResultDetail(task_id=_content.task_id, state=TaskResult.Error.value, status_code=500, response=None, error_msg=f"{e}"))
            else:
                # Update attributes with successful result
                _running_result = TaskContentDataUtils.convert_to_running_result(_original_task.running_result)
                _updated_running_result = RunningResult(success_count=_running_result.success_count + 1, fail_count=_running_result.fail_count)

                _result_detail = _original_task.result_detail
                _result_detail.append(ResultDetail(task_id=_content.task_id, state=TaskResult.Done.value, status_code=200, response=_data, error_msg=None))

            _current_task = Update.task(task=_original_task, in_progressing_id=_content.task_id, running_result=_updated_running_result, result_detail=_result_detail)
            self._set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_current_task)

        # Finish all tasks, record the running result and reset the content ...
        _current_task = Update.task(task=_current_task, running_content=[], in_progressing_id="-1", running_status=TaskResult.Done)
        self._set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_current_task)

    def processing_crawling_task(self, content: RunningContent) -> Any:
        _parsed_response = self.crawl(method=content.method, url=content.url)
        _data = self.data_process(_parsed_response)
        # self.persist(data=_data)
        return _data

    def discover(self, crawler_name: str, heartbeat: Heartbeat) -> Task:
        print(f"[DEBUG - discover]")
        _node_state_path = f"{self._generate_path(crawler_name)}/{self._Zookeeper_NodeState_Node_Path}"
        _node_state = self._get_metadata_from_zookeeper(path=_node_state_path, as_obj=NodeState)
        _node_state.role = CrawlerStateRole.Dead_Runner
        self._set_metadata_to_zookeeper(path=_node_state_path, metadata=_node_state)

        _task = self._get_metadata_from_zookeeper(path=f"{self._generate_path(crawler_name)}/{self._Zookeeper_Task_Node_Path}", as_obj=Task)
        heartbeat.healthy_state = HeartState.Asystole
        heartbeat.task_state = _task.running_status
        self._set_metadata_to_zookeeper(path=f"{self._generate_path(crawler_name)}/{self._Zookeeper_Heartbeat_Node_Path}", metadata=heartbeat)

        return _task

    def activate(self, crawler_name: str, task: Task):
        print(f"[DEBUG - activate]")
        _node_state = self._get_metadata_from_zookeeper(path=self.node_state_zookeeper_path, as_obj=NodeState)
        self._crawler_role = CrawlerStateRole.Runner
        _node_state.role = self._crawler_role
        self._set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_node_state)

        with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock, identifier=self._state_identifier):
            _state = self._get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)

            _state.total_backup = _state.total_backup - 1
            _state.current_crawler.remove(crawler_name)
            _state.current_runner.remove(crawler_name)
            _state.current_runner.append(self._crawler_name)
            _state.current_backup.remove(self._crawler_name)
            _state.fail_crawler.append(crawler_name)
            _state.fail_runner.append(crawler_name)
            _state.standby_id = str(int(_state.standby_id) + 1)

            self._set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state)

        if task.result_detail == TaskResult.Processing.value:
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
            _heartbeat = Initial.heartbeat(update_time="0.4s", update_timeout="0.8s", heart_rhythm_timeout="3")
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
                self._standby_id = _updated_state.standby_id
            else:
                raise ValueError(f"It doesn't support {role} recently.")

            self._set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_updated_state)

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
                    _heartbeat = Update.heartbeat(_heartbeat, heart_rhythm_time=datetime.now(), healthy_state=HeartState.Healthy, task_state=_task.running_status)
                    self._set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat)

                    # Sleep ...
                    time.sleep(self._get_sleep_time(_heartbeat.update_timeout))
                except Exception as e:
                    self.__Updating_Exception = e
                    break
            else:
                _task = self._get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                _heartbeat = self._get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)
                _heartbeat = Update.heartbeat(_heartbeat, heart_rhythm_time=datetime.now(), healthy_state=HeartState.ApparentDeath, task_state=_task.running_status)
                self._set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat)
                break
        if self.__Updating_Exception is not None:
            raise self.__Updating_Exception

    def _run_crawling_processing(self, content: RunningContent) -> Any:
        if self._chk_register(persist=False) is True:
            _data = self.processing_crawling_task(content)
            return _data
        else:
            raise NotImplementedError("You should implement the SmoothCrawler components and register them.")

    def _chk_register(self, http_sender: bool = True, http_resp_parser: bool = True, data_hdler: bool = True, persist: bool = True) -> bool:

        def _is_not_none(_chk: bool, _val) -> bool:
            if _chk is True:
                return _val is not None
            return True

        _factory = self._factory
        return _is_not_none(http_sender, _factory.http_factory) and \
               _is_not_none(http_resp_parser, _factory.parser_factory) and \
               _is_not_none(data_hdler, _factory.data_handling_factory) and \
               _is_not_none(persist, _factory.data_handling_factory)

    def _get_metadata_from_zookeeper(self, path: str, as_obj: Type[_BaseMetaDataType], must_has_data: bool = True) -> Generic[_BaseMetaDataType]:
        _value = self._Zookeeper_Client.get_value_from_node(path=path)
        if ZookeeperCrawler._value_is_not_empty(_value):
            _state = self._zk_converter.deserialize_meta_data(data=_value, as_obj=as_obj)
            return _state
        else:
            if must_has_data is True:
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
            else:
                return None

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
    def _get_sleep_time(cls, timer: str) -> Union[int, float]:
        _timer_val = timer[:-1]
        if "." in _timer_val:
            _time = float(_timer_val)
        else:
            _time = int(_timer_val)
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
