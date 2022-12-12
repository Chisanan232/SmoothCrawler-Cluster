from smoothcrawler.crawler import BaseCrawler
from smoothcrawler.factory import BaseFactory
from datetime import datetime
from typing import List, Dict, Callable, Any, Type, TypeVar, Optional, Generic
from abc import ABCMeta
import threading
import time
import re

from .model import (
    # Zookeeper operating common functions
    Initial, Update,
    # Enum objects
    CrawlerStateRole, TaskResult, HeartState,
    # Content data namedtuple object
    RunningContent, RunningResult, ResultDetail,
    # Meta data objects
    GroupState, NodeState, Task, Heartbeat
)
from .model.metadata import _BaseMetaData
from .election import BaseElection, IndexElection, ElectionResult
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

    __Initial_Standby_ID: str = "0"

    def __init__(self, runner: int, backup: int, name: str = "", group: str = "", index_sep: List[str] = ["-", "_"],
                 initial: bool = True, ensure_initial: bool = False, ensure_timeout: int = 3, ensure_wait: float = 0.5,
                 zk_hosts: str = None, zk_converter: Type[BaseConverter] = None,
                 election_strategy: Generic[BaseElectionType] = None,
                 factory: BaseFactory = None):
        """
        This is one of **_decentralized_** crawler cluster implementation with **Zookeeper**.

        :param runner: The number of crawler to run task. This value is equal to attribute _GroupState.total_runner_.
        :param backup: The number of crawler to check all crawler runner is alive or not and standby to activate by itself
                       to be another runner if anyone is dead. This value is equal to attribute _GroupState.total_backup_.
        :param name: The name of crawler instance. Default value is _sc-crawler_1_.
        :param group: The group name this crawler instance would belong to it. Default value is _sc-crawler-cluster_.
        :param index_sep: The separator of what index is in _name_. It only accepts 2 types: dash ('-') or under line ('_').
        :param initial: It would run initial processing if this option is True. The initial processing detail procedure is:
                        register meta-data to Zookeeper -> activate and run a new thread about keeping updating heartbeat
                        info -> wait and check whether it's ready for running election or not -> run election of task runner
                        -> update role
        :param ensure_initial: If it's True, it would guarantee the value of register meta-data processing is satisfied
                               of size of _GroupState.current_crawler_ is equal to the total of runner and backup, and
                               this crawler name must be in it.
        :param ensure_timeout: The times of timeout to guarantee the register meta-data processing finish. Default value is 3.
        :param ensure_wait: How long to wait between every checking. Default value is 0.5 (unit is second).
        :param zk_hosts: The Zookeeper hosts. Use comma to separate each hosts if it has multiple values. Default value is _localhost:2181_.
        :param zk_converter: The converter to parse data content to be an object. It must be a type of **_BaseConverter_**.
                             Default value is **_JsonStrConverter_**.
        :param election_strategy: The strategy of election. Default strategy is **_IndexElection_**.
        :param factory: The factory which saves SmoothCrawler components.
        """

        super(ZookeeperCrawler, self).__init__(factory=factory)

        self._total_crawler = runner + backup
        self._runner = runner
        self._backup = backup
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
        self._zk_hosts = zk_hosts
        self._Zookeeper_Client = ZookeeperClient(hosts=self._zk_hosts)

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
    def name(self) -> str:
        """
        This crawler instance name. It MUST be unique naming in cluster (the same group) for let entire crawler cluster
        to distinguish every one, for example, the properties _current_crawler_, _current_runner_ and _current_backup_ in
        meta-data **GroupState** would record by crawler names.
        This option value could be modified by Zookeeper object option _name_.

        :return: A string type value.
        """

        return self._crawler_name

    @property
    def group(self) -> str:
        """
        The group name of this crawler instance. This also means the cluster naming to let system distinguishes which crawler
        instance belong to which cluster.
        This option value could be modified by Zookeeper object option _group_.

        :return: A string type value.
        """

        return self._crawler_group

    @property
    def role(self) -> CrawlerStateRole:
        """
        The role of crawler instance. Please refer to _CrawlerStateRole_ to get more detail of it.

        :return: The current crawler role it is.
        """

        return self._crawler_role

    @property
    def zookeeper_hosts(self) -> str:
        """
        The Zookeeper hosts.

        :return: A string type value which should be as IP:Port (multi: IP:Port,IP:Port) format content.
        """

        return self._zk_hosts

    @property
    def ensure_register(self) -> bool:
        """
        The getter and setter of option _ensure_initial_.
        """

        return self._ensure_register

    @ensure_register.setter
    def ensure_register(self, ensure: bool) -> None:
        self._ensure_register = ensure

    @property
    def ensure_timeout(self) -> int:
        """
        The getter and setter of option _ensure_timeout_.
        """

        return self._ensure_timeout

    @ensure_timeout.setter
    def ensure_timeout(self, timeout: float) -> None:
        self._ensure_timeout = timeout

    @property
    def ensure_wait(self) -> float:
        """
        The getter and setter of option _ensure_wait_.
        """

        return self._ensure_wait

    @ensure_wait.setter
    def ensure_wait(self, wait: int) -> None:
        self._ensure_wait = wait

    @property
    def group_state_zookeeper_path(self) -> str:
        """
        The node path of meta-data _GroupState_ in Zookeeper.
        """

        return f"{self._generate_path(self._crawler_group, is_group=True)}/{self._Zookeeper_GroupState_Node_Path}"

    @property
    def node_state_zookeeper_path(self) -> str:
        """
        The node path of meta-data _NodeState_ in Zookeeper.
        """

        return f"{self._generate_path(self._crawler_name)}/{self._Zookeeper_NodeState_Node_Path}"

    @property
    def task_zookeeper_path(self) -> str:
        """
        The node path of meta-data _Task_ in Zookeeper.
        """

        return f"{self._generate_path(self._crawler_name)}/{self._Zookeeper_Task_Node_Path}"

    @property
    def heartbeat_zookeeper_path(self) -> str:
        """
        The node path of meta-data _Heartbeat_ in Zookeeper.
        """

        return f"{self._generate_path(self._crawler_name)}/{self._Zookeeper_Heartbeat_Node_Path}"

    @classmethod
    def _generate_path(cls, crawler_name: str, is_group: bool = False) -> str:
        """
        Generate node path of Zookeeper with fixed format.

        :param crawler_name: The crawler name.
        :param is_group: If it's True, generate node path for _group_ type meta-data.
        :return: A string type value with fixed format.
        """

        if is_group is True:
            return f"smoothcrawler/group/{crawler_name}"
        else:
            return f"smoothcrawler/node/{crawler_name}"

    def initial(self) -> None:
        """
        Initial processing of entire cluster running. This processing procedure like below:

        <Has an image of workflow>

        * Register
        Initial each needed meta-data (_GroupState_, _NodeState_, _Task_ and _Heartbeat_) and set them to Zookeeper, it
        also creates their node if it doesn't exist in Zookeeper. This processing about setting meta-data to Zookeeper be
        called _register_.

        * Activate and run new thread to keep updating heartbeat info
        If the signal _\_\_Updating_Stop_Signal_ is False, it would create a new thread here and it only keeps doing one
        thing again and again --- updating it owns heartbeat info by itself.

        * Check it's ready for running election
        There are 2 conditions to check whether it is ready or not:
            1. The size of _GroupState.current_crawler_ is equal to the sum of runner and backup.
            2. Current crawler instance's name be included in the list _GroupState.current_crawler_.
        The checking is forever (option _timeout_ is -1) and it only waits 0.5 seconds between every checks.

        * Run election for runner
        Elect which ones are runner and rest ones are backup. It could exchange different strategies to let election has
        different win conditions.

        * Update the role of crawler instance
        After election, everyone must know what role it is, and they would update this info to Zookeeper to prepare for
        doing their own jobs.

        :return: None
        """

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
        """
        Register all needed meta-data (_GroupState_, _NodeState_, _Task_ and _Heartbeat_) to Zookeeper.

        :return: None
        """

        # Question: How could it name the web crawler with number?
        # Current Answer:
        # 1. Index: get data from Zookeeper first and check the value, and it set the next index of crawler and save it to Zookeeper.
        # 2. Hardware code: Use the unique hardware code or flag to record it, i.e., the MAC address of host or something ID of container.
        self.register_group_state()
        self.register_node_state()
        self.register_task()
        self.register_heartbeat()

    def register_group_state(self) -> None:
        """
        Register meta-data _GroupState_ to Zookeeper. This processing is more difficult than others because it's a common
        meta-data for all crawler instances in cluster to refer. So here processing we need to wait everyone sync their
        info to it, in other words, it runs synchronously to ensure they won't cover other's values by each others.

        It also has a flag _ensure_register_. If it's True, it would double check the meta-data would be reasonable by
        specific conditions:

            1. The size of _GroupState.current_crawler_ is equal to the sum of runner and backup.
            2. Current crawler instance's name be included in the list _GroupState.current_crawler_.

        :return: None
        """

        for _ in range(self._ensure_timeout):
            with self._Zookeeper_Client.restrict(
                    path=self.group_state_zookeeper_path,
                    restrict=ZookeeperRecipe.WriteLock,
                    identifier=self._state_identifier):
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
                            standby_id=self.__Initial_Standby_ID
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
            raise TimeoutError(f"It gets timeout of registering meta data *GroupState* to Zookeeper cluster '{self.zookeeper_hosts}'.")

    def register_node_state(self) -> None:
        """
        Register meta-data _NodeState_ to Zookeeper.

        :return: None
        """

        _state = Initial.node_state(group=self._crawler_group)
        if self._Zookeeper_Client.exist_node(path=self.node_state_zookeeper_path) is None:
            _create_node = True
        else:
            _create_node = False
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_state, create_node=_create_node)

    def register_task(self) -> None:
        """
        Register meta-data _Task_ to Zookeeper.

        :return: None
        """

        _task = Initial.task()
        if self._Zookeeper_Client.exist_node(path=self.task_zookeeper_path) is None:
            _create_node = True
        else:
            _create_node = False
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_task, create_node=_create_node)

    def register_heartbeat(self) -> None:
        """
        Register meta-data _Heartbeat_ to Zookeeper.

        :return: None
        """

        # TODO: It needs to parameterize these settings
        _heartbeat = Initial.heartbeat(update_time="0.5s", update_timeout="2s", heart_rhythm_timeout="3")
        if self._Zookeeper_Client.exist_node(path=self.heartbeat_zookeeper_path) is None:
            _create_node = True
        else:
            _current_heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)
            _create_node = False
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat, create_node=_create_node)

    def stop_update_heartbeat(self) -> None:
        """
        Set the flag of _\_\_Updating_Stop_Signal_ to be True.

        :return: None
        """

        self.__Updating_Stop_Signal = True

    def is_ready_for_election(self, interval: float = 0.5, timeout: float = -1) -> bool:
        """
        Check whether it is ready to run next processing for function **_elect_**.

        :param interval: How long it should wait between every check. Default value is 0.5 (unit is seconds).
        :param timeout: Waiting timeout, if it's -1 means it always doesn't time out. Default value is -1.
        :return: A boolean type value. if it's True, means that it's ready to run next processing.
        """

        def _chk_by_condition(_state: GroupState) -> bool:
            return len(_state.current_crawler) == self._total_crawler and self._crawler_name in _state.current_crawler

        return self._is_ready_by_groupstate(
            condition_callback=_chk_by_condition,
            interval=interval,
            timeout=timeout
        )

    def is_ready_for_run(self, interval: float = 0.5, timeout: float = -1) -> bool:
        """
        Check whether it is ready to run next processing for function **_run_**.

        :param interval: How long it should wait between every check. Default value is 0.5 (unit is seconds).
        :param timeout: Waiting timeout, if it's -1 means it always doesn't time out. Default value is -1.
        :return: A boolean type value. if it's True, means that it's ready to run next processing.
        """

        def _chk_by_condition(_state: GroupState) -> bool:
            return len(_state.current_crawler) == self._total_crawler and self._crawler_name in _state.current_crawler and \
                   len(_state.current_runner) == self._runner and len(_state.current_backup) == self._backup

        return self._is_ready_by_groupstate(
            condition_callback=_chk_by_condition,
            interval=interval,
            timeout=timeout
        )

    def _is_ready_by_groupstate(self, condition_callback: Callable, interval: float = 0.5, timeout: float = -1) -> bool:
        """
        Use the _condition_callback_ option to judge it is ready for next running or not. The callback function must have
        an argument is _GroupState_.

        In cluster running, it has some scenarios would need to wait for data be synced successfully and finish. And the
        scenarios in SmoothCrawler-Cluster would deeply depend on meta-data _GroupState_.

        :param condition_callback: The callback function of checking and judge it's ready to run next process or not.
        :param interval: How long it should wait between every check. Default value is 0.5 (unit is seconds).
        :param timeout: Waiting timeout, if it's -1 means it always doesn't time out. Default value is -1.
        :return: A boolean type value. if it's True, means that it's ready to run next processing.
        """

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
        """
        Run election to choose which crawler instances are runner and rest ones are backup.

        :return: A **ElectionResult** type value.
        """

        _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
        return self._election_strategy.elect(candidate=self._crawler_name, member=_state.current_crawler,
                                             index_sep=self._index_sep, spot=self._runner)

    def run(self) -> None:
        """
        The major function of the cluster. It has a simple workflow cycle:

        <has an image of the workflow>

        :return: None
        """

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
        """
        Pre-processing which would be run first of all. Default implementation is doing nothing.

        :return: None
        """

        pass

    def running_as_role(self, role: CrawlerStateRole, wait_task_time: int = 2) -> None:
        """
        Running the crawler instance's own job by what role it is.

        * _CrawlerStateRole.Runner_ ->
            waiting for tasks and run it if it gets.
        * _CrawlerStateRole.Backup_Runner_ with index is standby_id ->
            keep checking every runner's heartbeat and standby to activate to be a runner by itself if it discovers anyone is dead.
        * _CrawlerStateRole.Backup_Runner_ with index is not standby_id ->
            keep checking the standby ID, and to be primary backup if the standby ID is equal to its index.

        :param role: The role of crawler instance.
        :param wait_task_time: How long does the crawler instance wait a second for next task. The unit is seconds and default value is 2.
        :return: None
        """

        if role is CrawlerStateRole.Runner:
            self.wait_for_task(wait_time=wait_task_time)
        elif role is CrawlerStateRole.Backup_Runner:
            _group_state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            if self._crawler_name.split(self._index_sep)[-1] == _group_state.standby_id:
                self.wait_and_standby()
            else:
                self.wait_for_to_be_standby()

    def before_dead(self, exception: Exception) -> None:
        """
        Do something when it gets an exception. The default implementation is raising the exception to outside.

        :param exception: The exception it got.
        :return: None
        """

        raise exception

    def wait_for_task(self, wait_time: int = 2) -> None:
        """
        Keep waiting for tasks coming and run it.

        :return: None
        """

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
                time.sleep(wait_time)

    def wait_and_standby(self) -> None:
        """
        Keep checking everyone's heartbeat info, and standby to activate to be a runner by itself if it discovers anyone
        is dead.

        It would record 2 types checking result as dict type value and the data structure like below:

            * timeout records: {<crawler_name>: <timeout times>}
            * not timeout records: {<crawler_name>: <not timeout times>}

        It would record the timeout times into the value, and it would get each types timeout threshold from meta-data
        _Heartbeat_. So it would base on them to run the checking process.

        Prevent from the times be counted by unstable network environment and cause the crawler instance be marked as _Dead_,
        it won't count the times by accumulation, it would reset the timeout value if the record be counted long time ago.
        Currently, it would reset the timeout value if it was counted 10 times ago.

        :return: None
        """

        _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)

        _timeout_records: Dict[str, int] = {}
        _no_timeout_records: Dict[str, int] = {}
        _detect_heart_rhythm_timeout: bool = False

        def _one_current_runner_is_dead(runner_name: str) -> Optional[str]:
            _current_runner_heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(
                path=f"{self._generate_path(runner_name)}/{self._Zookeeper_Heartbeat_Node_Path}",
                as_obj=Heartbeat
            )

            _heart_rhythm_time = _current_runner_heartbeat.heart_rhythm_time
            _time_format = _current_runner_heartbeat.time_format
            _update_timeout = _current_runner_heartbeat.update_timeout
            _heart_rhythm_timeout = _current_runner_heartbeat.heart_rhythm_timeout

            _diff_datetime = datetime.now() - datetime.strptime(_heart_rhythm_time, _time_format)
            if _diff_datetime.total_seconds() >= parse_timer(_update_timeout):
                # It should start to pay attention on it
                _timeout_records[runner_name] = _timeout_records.get(runner_name, 0) + 1
                _no_timeout_records[runner_name] = 0
                if _timeout_records[runner_name] >= int(_heart_rhythm_timeout):
                    return runner_name
            else:
                _no_timeout_records[runner_name] = _no_timeout_records.get(runner_name, 0) + 1
                # TODO: Maybe we could parameterize this option
                if _no_timeout_records[runner_name] >= 10:
                    _timeout_records[runner_name] = 0
            return None

        while True:
            _group_state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            _chk_current_runners_is_dead = map(_one_current_runner_is_dead, _group_state.current_runner)
            _dead_current_runner_iter = filter(lambda _dead_runner: _dead_runner is not None, list(_chk_current_runners_is_dead))
            _dead_current_runner = list(_dead_current_runner_iter)
            if len(_dead_current_runner) != 0:
                # Only handle the first one of dead crawlers (if it has multiple dead crawlers
                _runner_name = _dead_current_runner[0]
                _heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(
                    path=f"{self._generate_path(_runner_name)}/{self._Zookeeper_Heartbeat_Node_Path}",
                    as_obj=Heartbeat
                )
                _task_of_dead_crawler = self.discover(dead_crawler_name=_runner_name, heartbeat=_heartbeat)
                self.activate(dead_crawler_name=_runner_name)
                self.hand_over_task(task=_task_of_dead_crawler)
                break

            # TODO: Parameterize this value for sleep a little bit while.
            time.sleep(0.5)

    def wait_for_to_be_standby(self) -> bool:
        """
        Keep waiting to be the primary backup crawler instance.

        :return: It would return True if it directs the standby ID attribute value is equal to its index of name.
        """

        while True:
            _group_state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            if self._crawler_name.split(self._index_sep)[-1] == _group_state.standby_id:
                # Start to do wait_and_standby
                return True
            # TODO: Parameterize this value for sleep a little bit while.
            time.sleep(2)

    def run_task(self, task: Task) -> None:
        """
        Run the task it directs. It runs the task by meta-data _Task.running_content_ and records the running result back
        to meta-data _Task.running_result_ and _Task.result_detail_.

        The core implementation of how it works web spider job in protected function __run_crawling_processing_ (and _processing_crawling_task_).

        :param task: The task it directs.
        :return: None
        """

        _running_content = task.running_content
        _current_task: Task = task
        _start_task_id = task.in_progressing_id

        assert re.search(r"[0-9]{1,32}]", _start_task_id) is None, "The task index must be integer format value."

        for _index, _content in enumerate(_running_content[int(_start_task_id):]):
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
                _result_detail.append(
                    ResultDetail(task_id=_content.task_id, state=TaskResult.Error.value, status_code=500, response=None, error_msg=f"{e}"))
            else:
                # Update attributes with successful result
                _running_result = TaskContentDataUtils.convert_to_running_result(_original_task.running_result)
                _updated_running_result = RunningResult(success_count=_running_result.success_count + 1, fail_count=_running_result.fail_count)

                _result_detail = _original_task.result_detail
                _result_detail.append(
                    ResultDetail(task_id=_content.task_id, state=TaskResult.Done.value, status_code=200, response=_data, error_msg=None))

            _current_task = Update.task(task=_original_task, in_progressing_id=_content.task_id,
                                        running_result=_updated_running_result, result_detail=_result_detail)
            self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_current_task)

        # Finish all tasks, record the running result and reset the content ...
        _current_task = Update.task(task=_current_task, running_content=[], in_progressing_id="-1", running_status=TaskResult.Done)
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_current_task)

    def processing_crawling_task(self, content: RunningContent) -> Any:
        """
        The core of web spider implementation. All running functions it used are developers are familiar with --- SmoothCrawler components.

        :param content: A RunningContent type object which provides clear attributes to run crawling task.
        :return: Any type object (The running result of crawling).
        """

        # TODO: Consider about how to let crawler core implementation to be more scalable and flexible.
        _parsed_response = self.crawl(method=content.method, url=content.url)
        _data = self.data_process(_parsed_response)
        # self.persist(data=_data)
        return _data

    def discover(self, dead_crawler_name: str, heartbeat: Heartbeat) -> Task:
        """
        When backup role crawler instance discover anyone is dead, it would mark the target one as _Dead_ (_HeartState.Asystole_)
        and update its meta-data _Heartbeat_. In the same time, it would try to get its _Task_ and take over it.

        :param dead_crawler_name: The crawler name which be under checking.
        :param heartbeat: The meta-data _Heartbeat_ of crawler be under checking.
        :return: Meta-data _Task_ from dead crawler instance.
        """

        _node_state_path = f"{self._generate_path(dead_crawler_name)}/{self._Zookeeper_NodeState_Node_Path}"
        _node_state = self._MetaData_Util.get_metadata_from_zookeeper(path=_node_state_path, as_obj=NodeState)
        _node_state.role = CrawlerStateRole.Dead_Runner
        self._MetaData_Util.set_metadata_to_zookeeper(path=_node_state_path, metadata=_node_state)

        _task = self._MetaData_Util.get_metadata_from_zookeeper(path=f"{self._generate_path(dead_crawler_name)}/{self._Zookeeper_Task_Node_Path}", as_obj=Task)
        heartbeat.healthy_state = HeartState.Asystole
        heartbeat.task_state = _task.running_status
        self._MetaData_Util.set_metadata_to_zookeeper(path=f"{self._generate_path(dead_crawler_name)}/{self._Zookeeper_Heartbeat_Node_Path}", metadata=heartbeat)

        return _task

    def activate(self, dead_crawler_name: str) -> None:
        """
        After backup role crawler instance marks target as _Dead_, it would start to activate to be running by itself and
        run the runner's job.

        :param dead_crawler_name: The crawler name which be under checking.
        :return: None
        """

        _node_state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.node_state_zookeeper_path, as_obj=NodeState)
        self._crawler_role = CrawlerStateRole.Runner
        _node_state.role = self._crawler_role
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_node_state)

        with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock,
                                             identifier=self._state_identifier):
            _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)

            _state.total_backup = _state.total_backup - 1
            _state.current_crawler.remove(dead_crawler_name)
            _state.current_runner.remove(dead_crawler_name)
            _state.current_runner.append(self._crawler_name)
            _state.current_backup.remove(self._crawler_name)
            _state.fail_crawler.append(dead_crawler_name)
            _state.fail_runner.append(dead_crawler_name)
            _state.standby_id = str(int(_state.standby_id) + 1)

            self._MetaData_Util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_state)

    def hand_over_task(self, task: Task) -> None:
        """
        Hand over the task of the dead crawler instance. It would get the meta-data _Task_ from dead crawler and write it
        to this crawler's meta-data _Task_.

        Args:
            task: The meta-data _Task_ of crawler be under checking.

        Returns:
            None

        """

        if task.running_status == TaskResult.Processing.value:
            # Run the tasks from the start index
            self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=task)
        elif task.running_status in [TaskResult.Nothing.value, TaskResult.Error.value]:
            # Reset some specific attributes
            _updated_task = Update.task(task, in_progressing_id='0', running_result=RunningResult(success_count=0, fail_count=0), result_detail=[])
            # Reruns all tasks
            self._MetaData_Util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=_updated_task)
        else:
            # Ignore and don't do anything if the task state is nothing or done.
            pass

    def _update_crawler_role(self, role: CrawlerStateRole) -> None:
        """
        Update to be what role current crawler instance is in crawler cluster.

        :param role: The role of crawler instance.
        :return: None
        """

        _node_state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.node_state_zookeeper_path, as_obj=NodeState)
        _updated_node_state = Update.node_state(node_state=_node_state, role=role)
        self._MetaData_Util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=_updated_node_state)

        with self._Zookeeper_Client.restrict(path=self.group_state_zookeeper_path, restrict=ZookeeperRecipe.WriteLock,
                                             identifier=self._state_identifier):
            _state = self._MetaData_Util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path, as_obj=GroupState)
            if role is CrawlerStateRole.Runner:
                _updated_state = Update.group_state(
                    _state,
                    append_current_runner=[self._crawler_name],
                )
            elif role is CrawlerStateRole.Backup_Runner:
                _crawler_index = self._crawler_name.split(self._index_sep)[-1]
                _current_standby_id = _state.standby_id
                if int(_crawler_index) > int(_current_standby_id) and _current_standby_id != self.__Initial_Standby_ID:
                    _updated_state = Update.group_state(
                        _state,
                        append_current_backup=[self._crawler_name]
                    )
                else:
                    _updated_state = Update.group_state(
                        _state,
                        append_current_backup=[self._crawler_name],
                        standby_id=self._crawler_name.split(self._index_sep)[-1]
                    )
            else:
                raise ValueError(f"It doesn't support {role} recently.")

            self._MetaData_Util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=_updated_state)

    def _run_updating_heartbeat_thread(self) -> None:
        """
        Activate and run a new thread to keep updating _Heartbeat_ info.

        :return: None
        """

        _updating_heartbeat_thread = threading.Thread(target=self._update_heartbeat)
        _updating_heartbeat_thread.daemon = True
        _updating_heartbeat_thread.start()

    def _update_heartbeat(self) -> None:
        """
        The main function of updating _Heartbeat_ info.
        It has a flag _\_\_Updating_Exception_. If it's True, it would stop updating _Heartbeat_.

        :return: None
        """

        while True:
            if self.__Updating_Stop_Signal is False:
                try:
                    # Get *Task* and *Heartbeat* info
                    _task = self._MetaData_Util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                    _heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)

                    # Update the values
                    _heartbeat = Update.heartbeat(_heartbeat, heart_rhythm_time=datetime.now(),
                                                  healthy_state=HeartState.Healthy, task_state=_task.running_status)
                    self._MetaData_Util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat)

                    # Sleep ...
                    time.sleep(parse_timer(_heartbeat.update_time))
                except Exception as e:
                    self.__Updating_Exception = e
                    break
            else:
                _task = self._MetaData_Util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                _heartbeat = self._MetaData_Util.get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path, as_obj=Heartbeat)
                _heartbeat = Update.heartbeat(_heartbeat, heart_rhythm_time=datetime.now(),
                                              healthy_state=HeartState.ApparentDeath, task_state=_task.running_status)
                self._MetaData_Util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=_heartbeat)
                break
        if self.__Updating_Exception is not None:
            raise self.__Updating_Exception

    def _run_crawling_processing(self, content: RunningContent) -> Any:
        """
        The wrapper function of core crawling function _processing_crawling_task_. This meaning is checking the SmoothCrawler
        components have been registered or not.

        :param content: A RunningContent type object which provides clear attributes to run crawling task.
        :return: Any type object (The running result of crawling).
        """

        if self._chk_register(persist=False) is True:
            _data = self.processing_crawling_task(content)
            return _data
        else:
            raise NotImplementedError("You should implement the SmoothCrawler components and register them.")

    def _chk_register(self, http_sender: bool = True, http_resp_parser: bool = True, data_hdler: bool = True, persist: bool = True) -> bool:
        """
        Checking the SmoothCrawler components has been registered or not.

        :param http_sender: Checking component _CrawlerFactory.http_factory_ if it;s True.
        :param http_resp_parser: Checking component _CrawlerFactory.parser_factory_ if it;s True.
        :param data_hdler: Checking component _CrawlerFactory.data_handling_factory_ if it;s True.
        :param persist: Checking component _CrawlerFactory.data_handling_factory_ if it;s True.
        :return: A boolean type value. If it's True, means SmoothCrawler have been registered.
        """

        def _is_not_none(_chk: bool, _val) -> bool:
            if _chk is True:
                return _val is not None
            return True

        _factory = self._factory
        return _is_not_none(http_sender, _factory.http_factory) and \
               _is_not_none(http_resp_parser, _factory.parser_factory) and \
               _is_not_none(data_hdler, _factory.data_handling_factory) and \
               _is_not_none(persist, _factory.data_handling_factory)
