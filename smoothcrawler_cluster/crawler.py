from smoothcrawler.crawler import BaseCrawler
from smoothcrawler.factory import BaseFactory
from datetime import datetime
from typing import List, Dict, Callable, Any, Type, TypeVar, Union, Generic
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
from ._utils import parse_timer, MetaDataUtil
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
    _MetaData_Util: MetaDataUtil = None

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

        self._MetaData_Util = MetaDataUtil(client=self._Zookeeper_Client, converter=self._zk_converter)

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
            self.initial()

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

    def initial(self) -> None:
        self.register_metadata()
        if self.__Updating_Stop_Signal is False:
            self._run_updating_heartbeat_thread()
        if self.is_ready_for_election(interval=0.5, timeout=-1):
            if self.elect() is ElectionResult.Winner:
                self._crawler_role = CrawlerStateRole.Runner
            else:
                self._crawler_role = CrawlerStateRole.Backup_Runner
            self._update_crawler_role(self._crawler_role)
        else:
            raise TimeoutError("Timeout to wait for crawler be ready in register process.")

    def register_metadata(self) -> None:
        # Question: How could it name the web crawler with number?
        # Current Answer:
        # 1. Index: get data from Zookeeper first and check the value, and it set the next index of crawler and save it to Zookeeper.
        # 2. Hardware code: Use the unique hardware code or flag to record it, i.e., the MAC address of host or something ID of container.
        self.register_group_state()
        self.register_node_state()
        self.register_task()
        self.register_heartbeat()

    def register_group_state(self) -> None:
        for _ in range(self._ensure_timeout):
            with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock, identifier=self._state_identifier):
                if self._Zookeeper_Client.exist_node(path=self.group_state_zookeeper_path) is None:
                    _state = Initial.group_state(
                        crawler_name=self._crawler_name,
                        total_crawler=self._runner + self._backup,
                        total_runner=self._runner,
                        total_backup=self._backup
                    )
                    self._MetaData_Util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state, create_node=True)
                else:
                    _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
                    if _state.current_crawler is None or self._crawler_name not in _state.current_crawler:
                        _state = Update.group_state(
                            _state,
                            total_crawler=self._runner + self._backup,
                            total_runner=self._runner,
                            total_backup=self._backup,
                            append_current_crawler=[self._crawler_name],
                            standby_id="0"
                        )
                        self._MetaData_Util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state)

            if self._ensure_register is False:
                break

            _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            assert _state is not None, "The meta data *State* should NOT be None."
            if len(set(_state.current_crawler)) == self._total_crawler and self._crawler_name in _state.current_crawler:
                break
            if self._ensure_wait is not None:
                time.sleep(self._ensure_wait)
        else:
            raise TimeoutError(f"It gets timeout of registering meta data *State* to Zookeeper cluster '{self.zookeeper_hosts}'.")

    def register_node_state(self) -> None:
        if self._Zookeeper_Client.exist_node(path=self.node_state_zookeeper_path) is None:
            _state = Initial.node_state(group=self._crawler_group)
            self._MetaData_Util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_state, create_node=True)

    def register_task(self) -> None:
        if self._Zookeeper_Client.exist_node(path=self.task_zookeeper_path) is None:
            _task = Initial.task()
            self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_task, create_node=True)

    def register_heartbeat(self) -> None:
        if self._Zookeeper_Client.exist_node(path=self.heartbeat_zookeeper_path) is None:
            # TODO: It needs to parameterize these settings
            _heartbeat = Initial.heartbeat(update_time="0.5s", update_timeout="2s", heart_rhythm_timeout="3")
            _create_node = True
        else:
            _current_heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)
            _heartbeat = Update.heartbeat(_current_heartbeat, update_time="0.5s", update_timeout="2s", heart_rhythm_timeout="3")
            _create_node = False
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat, create_node=_create_node)

    def stop_update_heartbeat(self) -> None:
        self.__Updating_Stop_Signal = True

    def is_ready_for_election(self, interval: float = 0.5, timeout: float = -1) -> bool:

        def _chk_by_condition(_state: GroupState) -> bool:
            return len(_state.current_crawler) == self._total_crawler and self._crawler_name in _state.current_crawler

        return self._is_ready_by_groupstate(
            condition_callback=_chk_by_condition,
            interval=interval,
            timeout=timeout
        )

    def is_ready_for_run(self, interval: float = 0.5, timeout: float = -1) -> bool:

        def _chk_by_condition(_state: GroupState) -> bool:
            return len(_state.current_crawler) == self._total_crawler and self._crawler_name in _state.current_crawler and \
                   len(_state.current_runner) == self._runner and len(_state.current_backup) == self._backup

        return self._is_ready_by_groupstate(
            condition_callback=_chk_by_condition,
            interval=interval,
            timeout=timeout
        )

    def _is_ready_by_groupstate(self, condition_callback: Callable, interval: float = 0.5, timeout: float = -1) -> bool:
        if timeout < -1:
            raise ValueError("The option *timeout* value is incorrect. Please configure more than -1, and -1 means it never timeout.")

        _start = time.time()
        while True:
            _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            if condition_callback(_state) is True:
                return True
            if timeout != -1:
                if (time.time() - _start) >= timeout:
                    return False
            time.sleep(interval)

    def elect(self) -> ElectionResult:
        _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
        return self._election_strategy.elect(candidate=self._crawler_name, member=_state.current_crawler, index_sep=self._index_sep, spot=self._runner)

    def run(self) -> None:
        if self.is_ready_for_run(interval=0.5, timeout=-1) is True:
            while True:
                self.pre_running()
                try:
                    self.running_as_role(self._crawler_role)
                except Exception as e:
                    self.before_dead(e)
                time.sleep(1)
        else:
            raise TimeoutError("Timeout to wait for crawler be ready for running crawler cluster.")

    def pre_running(self) -> None:
        pass

    def running_as_role(self, role: CrawlerStateRole) -> None:
        if role is CrawlerStateRole.Runner:
            self.wait_for_task()
        elif role is CrawlerStateRole.Backup_Runner:
            if self._crawler_name.split(self._index_sep)[-1] == self._standby_id:
                self.wait_and_standby()
            else:
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
            _task = self._MetaData_Util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task, must_has_data=False)
            if _task.running_content is not None and len(_task.running_content) >= 1:
                # Start to run tasks ...
                self.run_task(task=_task)
            else:
                # Keep waiting
                # TODO: Parameterize this value for sleep a little bit while.
                time.sleep(2)

    def wait_and_standby(self) -> None:
        _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
        if _state.current_crawler is None or len(_state.current_crawler) != self._total_crawler or \
                _state.current_runner is None or len(_state.current_runner) != self._runner or \
                _state.current_backup is None or len(_state.current_backup) != self._backup:
            raise ZookeeperCrawlerNotReady

        _timeout_records: Dict[str, int] = {}
        _no_timeout_records: Dict[str, int] = {}
        _detect_heart_rhythm_timeout: bool = False

        def _chk_current_runner_heartbeat(runner_name: str) -> bool:
            _heartbeat_path = f"{self._generate_path(runner_name)}/{self._Zookeeper_Heartbeat_Node_Path}"
            _heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(path=_heartbeat_path, as_obj=Heartbeat)

            _heart_rhythm_time = _heartbeat.heart_rhythm_time
            _time_format = _heartbeat.time_format
            _update_timeout = _heartbeat.update_timeout
            _heart_rhythm_timeout = _heartbeat.heart_rhythm_timeout

            _diff_datetime = datetime.now() - datetime.strptime(_heart_rhythm_time, _time_format)
            if _diff_datetime.total_seconds() >= parse_timer(_update_timeout):
                # It should start to pay attention on it
                _timeout_records[runner_name] = _timeout_records.get(runner_name, 0) + 1
                _no_timeout_records[runner_name] = 0
                if _timeout_records[runner_name] >= int(_heart_rhythm_timeout):
                    # It should mark the runner as dead and try to activate itself.
                    _task_of_dead_crawler = self.discover(crawler_name=runner_name, heartbeat=_heartbeat)
                    self.activate(crawler_name=runner_name, task=_task_of_dead_crawler)
                    return True
            else:
                _no_timeout_records[runner_name] = _no_timeout_records.get(runner_name, 0) + 1
                # TODO: Maybe we could parameterize this option
                if _no_timeout_records[runner_name] >= 10:
                    _timeout_records[runner_name] = 0
            return False

        while True:
            _group_state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            _current_runners_heartbeat = map(_chk_current_runner_heartbeat, _group_state.current_runner)
            _current_runners_heartbeat_chksum = list(_current_runners_heartbeat)
            if True in _current_runners_heartbeat_chksum:
                break

            # TODO: Parameterize this value for sleep a little bit while.
            time.sleep(0.5)

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
            _original_task = self._MetaData_Util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
            if _index == 0:
                _current_task = Update.task(task=_original_task, in_progressing_id=_content.task_id, running_status=TaskResult.Processing)
            else:
                _current_task = Update.task(task=_original_task, in_progressing_id=_content.task_id)
            self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_current_task)

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
            self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_current_task)

        # Finish all tasks, record the running result and reset the content ...
        _current_task = Update.task(task=_current_task, running_content=[], in_progressing_id="-1", running_status=TaskResult.Done)
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_current_task)

    def processing_crawling_task(self, content: RunningContent) -> Any:
        _parsed_response = self.crawl(method=content.method, url=content.url)
        _data = self.data_process(_parsed_response)
        # self.persist(data=_data)
        return _data

    def discover(self, crawler_name: str, heartbeat: Heartbeat) -> Task:
        _node_state_path = f"{self._generate_path(crawler_name)}/{self._Zookeeper_NodeState_Node_Path}"
        _node_state = self._MetaData_Util.get_metadata_from_zookeeper(path=_node_state_path, as_obj=NodeState)
        _node_state.role = CrawlerStateRole.Dead_Runner
        self._MetaData_Util.set_metadata_to_zookeeper(path=_node_state_path, metadata=_node_state)

        _task = self._MetaData_Util.get_metadata_from_zookeeper(path=f"{self._generate_path(crawler_name)}/{self._Zookeeper_Task_Node_Path}", as_obj=Task)
        heartbeat.healthy_state = HeartState.Asystole
        heartbeat.task_state = _task.running_status
        self._MetaData_Util.set_metadata_to_zookeeper(path=f"{self._generate_path(crawler_name)}/{self._Zookeeper_Heartbeat_Node_Path}", metadata=heartbeat)

        return _task

    def activate(self, crawler_name: str, task: Task):
        _node_state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.node_state_zookeeper_path, as_obj=NodeState)
        self._crawler_role = CrawlerStateRole.Runner
        _node_state.role = self._crawler_role
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_node_state)

        with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock, identifier=self._state_identifier):
            _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)

            if self._crawler_name in _state.current_backup:
                _state.total_backup = _state.total_backup - 1
                _state.current_crawler.remove(crawler_name)
                _state.current_runner.remove(crawler_name)
                _state.current_runner.append(self._crawler_name)
                _state.current_backup.remove(self._crawler_name)
                _state.fail_crawler.append(crawler_name)
                _state.fail_runner.append(crawler_name)
                _state.standby_id = str(int(_state.standby_id) + 1)

                self._MetaData_Util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state)
            else:
                # TODO: Does it need to do something?
                # This crawler instance has been ready be activated by itself for others
                pass

        if task.result_detail == TaskResult.Processing.value:
            # TODO: Run task content ?
            pass
        else:
            # TODO: Do something else to handle?
            pass

    def _update_crawler_role(self, role: CrawlerStateRole) -> None:
        _node_state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.node_state_zookeeper_path, as_obj=NodeState)
        _updated_node_state = Update.node_state(node_state=_node_state, role=role)
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_updated_node_state)

        with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock, identifier=self._state_identifier):
            _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
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

            self._MetaData_Util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_updated_state)

    def _run_updating_heartbeat_thread(self) -> None:
        _updating_heartbeat_thread = threading.Thread(target=self._update_heartbeat)
        _updating_heartbeat_thread.daemon = True
        _updating_heartbeat_thread.start()

    def _update_heartbeat(self) -> None:
        while True:
            if self.__Updating_Stop_Signal is False:
                try:
                    # Get *Task* and *Heartbeat* info
                    _task = self._MetaData_Util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                    _heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)

                    # Update the values
                    _heartbeat = Update.heartbeat(_heartbeat, heart_rhythm_time=datetime.now(), healthy_state=HeartState.Healthy, task_state=_task.running_status)
                    self._MetaData_Util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat)

                    # Sleep ...
                    time.sleep(parse_timer(_heartbeat.update_timeout))
                except Exception as e:
                    self.__Updating_Exception = e
                    break
            else:
                _task = self._MetaData_Util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                _heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)
                _heartbeat = Update.heartbeat(_heartbeat, heart_rhythm_time=datetime.now(), healthy_state=HeartState.ApparentDeath, task_state=_task.running_status)
                self._MetaData_Util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat)
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
