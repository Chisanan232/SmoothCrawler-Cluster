"""*Different types cluster crawler with SmoothCrawler*

It integrates the features of **SmoothCrawler** into all the cluster crawler. So it has the features of
**SmoothCrawler**, and it also could extend the features to run in a cluster right now!
"""

from abc import ABCMeta
from datetime import datetime
from smoothcrawler.crawler import BaseCrawler
from smoothcrawler.factory import BaseFactory
from typing import List, Dict, Callable, Any, Type, TypeVar, Optional, Generic
import threading
import time
import re

from ._utils import parse_timer, MetaDataUtil
from ._utils.converter import BaseConverter, JsonStrConverter, TaskContentDataUtils
from ._utils.zookeeper import ZookeeperClient, ZookeeperRecipe
from .election import BaseElection, IndexElection, ElectionResult
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


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)
BaseElectionType = TypeVar("BaseElectionType", bound=BaseElection)


class BaseDistributedCrawler(metaclass=ABCMeta):
    """*Base class for distributed crawler*

    TODO: Add docstring, consider and define abstract functions
    """
    pass


class BaseDecentralizedCrawler(BaseDistributedCrawler):
    """*Base class for decentralized crawler*

    TODO: Add docstring, consider and define abstract functions
    """
    pass


class ZookeeperCrawler(BaseDecentralizedCrawler, BaseCrawler):
    """*Cluster crawler with Zookeeper*

    This crawler reaches cluster feature though third party application --- Zookeeper. So it let Zookeeper manage all
    processing of meta-data objects, e.g., *GroupState*, *Heartbeat*, etc.
    """

    _zookeeper_client: ZookeeperClient = None
    _default_zookeeper_hosts: str = "localhost:2181"
    _zookeeper_group_state_node_path: str = "state"
    _zookeeper_node_state_node_path: str = "state"
    _zookeeper_task_node_path: str = "task"
    _zookeeper_heartbeat_node_path: str = "heartbeat"

    _metadata_util: MetaDataUtil = None

    _initial_standby_id: str = "0"

    _updating_stop_signal: bool = False
    _updating_exception = None

    def __init__(
            self,
            runner: int,
            backup: int,
            name: str = "",
            group: str = "",
            index_sep: List[str] = ["-", "_"],
            initial: bool = True,
            ensure_initial: bool = False,
            ensure_timeout: int = 3,
            ensure_wait: float = 0.5,
            heartbeat_update: float = 0.5,
            heartbeat_update_timeout: float = 2,
            heartbeat_dead_threshold: int = 3,
            zk_hosts: Optional[str] = None,
            zk_converter: Optional[Type[BaseConverter]] = None,
            election_strategy: Generic[BaseElectionType] = None,
            factory: Optional[Type[BaseFactory]] = None,
    ):
        """

        Args:
            runner (int): The number of crawler to run task. This value is equal to attribute *GroupState.total_runner*.
            backup (int): The number of crawler to check all crawler runner is alive or not and standby to activate by
                itself to be another runner if anyone is dead. This value is equal to attribute GroupState.total_backup.
            name (str): The name of crawler instance. Default value is *sc-crawler_1*.
            group (str): The group name this crawler instance would belong to it. Default value is *sc-crawler-cluster*.
            index_sep (list of str): The separator of what index is in *name*. It only accepts 2 types: dash ('-') or
                under line ('_').
            initial (bool): It would run initial processing if this option is True. The initial processing detail
                procedure is:
                    register meta-data to Zookeeper -> activate and run a new thread about keeping updating heartbeat
                    info -> wait and check whether it's ready for running election or not -> run election of task runner
                    -> update role
            ensure_initial (bool): If it's True, it would guarantee the value of register meta-data processing is
                satisfied of size of *GroupState.current_crawler* is equal to the total of runner and backup, and
                this crawler name must be in it.
            ensure_timeout (int): The times of timeout to guarantee the register meta-data processing finish. Default
                value is 3.
            ensure_wait (float): How long to wait between every checking. Default value is 0.5 (unit is second).
            heartbeat_update (float): The time frequency to update heartbeat info, i.g., if value is '2', it would
                update heartbeat info every 2 seconds. The unit is seconds.
            heartbeat_update_timeout (float): The timeout value of updating, i.g., if value is '3', it is time out if it
                doesn't to update heartbeat info exceeds 3 seconds. The unit is seconds.
            heartbeat_dead_threshold (int): The threshold of timeout times to judge it is dead, i.g., if value is '3'
                and the updating timeout exceeds 3 times, it would be marked as 'Dead_<Role>' (like 'Dead_Runner' or
                'Dead_Backup').
            zk_hosts (str): The Zookeeper hosts. Use comma to separate each hosts if it has multiple values. Default
                value is *localhost:2181*.
            zk_converter (Type[BaseConverter]): The converter to parse data content to be an object. It must be a type
                of **BaseConverter**. Default value is **JsonStrConverter**.
            election_strategy (BaseElection): The strategy of election. Default strategy is **IndexElection**.
            factory (Type[BaseFactory]): The factory which saves SmoothCrawler components.
        """
        super().__init__(factory=factory)

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

        if not zk_hosts:
            zk_hosts = self._default_zookeeper_hosts
        self._zk_hosts = zk_hosts
        self._zookeeper_client = ZookeeperClient(hosts=self._zk_hosts)

        if not zk_converter:
            zk_converter = JsonStrConverter()
        self._zk_converter = zk_converter

        self._metadata_util = MetaDataUtil(client=self._zookeeper_client, converter=self._zk_converter)

        if not election_strategy:
            election_strategy = IndexElection()
        self._election_strategy = election_strategy
        if not self._index_sep:
            for sep in index_sep:
                crawler_name_list = self._crawler_name.split(sep=sep)
                if crawler_name_list:
                    # Checking the separating char is valid
                    try:
                        int(crawler_name_list[-1])
                    except ValueError:
                        continue
                    else:
                        self._index_sep = sep
                        self._crawler_index = crawler_name_list[-1]
                        self._election_strategy.identity = self._crawler_index
                        break
        else:
            crawler_name_list = self._crawler_name.split(sep=self._index_sep)
            self._crawler_index = crawler_name_list[-1]
            self._election_strategy.identity = self._crawler_index

        self._heartbeat_update = heartbeat_update
        self._heartbeat_update_timeout = heartbeat_update_timeout
        self._heartbeat_dead_threshold = heartbeat_dead_threshold

        if initial:
            self.initial()

    @property
    def name(self) -> str:
        """:obj:`str`: Properties with both a getter and setter. This crawler instance name. It MUST be unique naming
        in cluster (the same group) for let entire crawler cluster to distinguish every one, for example, the properties
        *current_crawler*, *current_runner* and *current_backup* in meta-data **GroupState** would record by crawler
        names. This option value could be modified by Zookeeper object option *name*.
        """
        return self._crawler_name

    @property
    def group(self) -> str:
        """:obj:`str`: Properties with both a getter and setter. The group name of this crawler instance. This also
        means the cluster naming to let system distinguishes which crawler instance belong to which cluster.
        This option value could be modified by Zookeeper object option *group*.
        """
        return self._crawler_group

    @property
    def role(self) -> CrawlerStateRole:
        """:obj:`str`: Properties with both a getter and setter. The role of crawler instance. Please refer to
        *CrawlerStateRole* to get more detail of it.
        """
        return self._crawler_role

    @property
    def zookeeper_hosts(self) -> str:
        """:obj:`str`: Properties with both a getter and setter. The Zookeeper hosts current crawler instance use to
        connect. The value should be as IP:Port (multi: IP:Port,IP:Port) format content.
        """
        return self._zk_hosts

    @property
    def ensure_register(self) -> bool:
        """:obj:`str`: Properties with both a getter and setter. The getter and setter of option *ensure_initial*.
        """
        return self._ensure_register

    @ensure_register.setter
    def ensure_register(self, ensure: bool) -> None:
        self._ensure_register = ensure

    @property
    def ensure_timeout(self) -> int:
        """:obj:`str`: Properties with both a getter and setter. The getter and setter of option *ensure_timeout*.
        """
        return self._ensure_timeout

    @ensure_timeout.setter
    def ensure_timeout(self, timeout: float) -> None:
        self._ensure_timeout = timeout

    @property
    def ensure_wait(self) -> float:
        """:obj:`str`: Properties with both a getter and setter. The getter and setter of option *ensure_wait*.
        """
        return self._ensure_wait

    @ensure_wait.setter
    def ensure_wait(self, wait: int) -> None:
        self._ensure_wait = wait

    @property
    def group_state_zookeeper_path(self) -> str:
        """:obj:`str`: Properties with both a getter and setter. The node path of meta-data **GroupState** in Zookeeper.
        """
        return f"{self._generate_path(self._crawler_group, is_group=True)}/{self._zookeeper_group_state_node_path}"

    @property
    def node_state_zookeeper_path(self) -> str:
        """:obj:`str`: Properties with both a getter and setter. The node path of meta-data **NodeState** in Zookeeper.
        """
        return f"{self._generate_path(self._crawler_name)}/{self._zookeeper_node_state_node_path}"

    @property
    def task_zookeeper_path(self) -> str:
        """:obj:`str`: Properties with both a getter and setter. The node path of meta-data **Task** in Zookeeper.
        """
        return f"{self._generate_path(self._crawler_name)}/{self._zookeeper_task_node_path}"

    @property
    def heartbeat_zookeeper_path(self) -> str:
        """:obj:`str`: Properties with both a getter and setter. The node path of meta-data **Heartbeat** in Zookeeper.
        """
        return f"{self._generate_path(self._crawler_name)}/{self._zookeeper_heartbeat_node_path}"

    @classmethod
    def _generate_path(cls, crawler_name: str, is_group: bool = False) -> str:
        """Generate node path of Zookeeper with fixed format.

        Args:
            crawler_name (str): The crawler name.
            is_group (bool): If it's True, generate node path for _group_ type meta-data.

        Returns:
            str: A Zookeeper node path.

        """
        if is_group:
            return f"smoothcrawler/group/{crawler_name}"
        else:
            return f"smoothcrawler/node/{crawler_name}"

    def initial(self) -> None:
        """Initial processing of entire cluster running. This processing procedure like below:

        <Has an image of workflow>

        * Register
        Initial each needed meta-data (*GroupState*, *NodeState*, *Task* and *Heartbeat*) and set them to Zookeeper,
        it also creates their node if it doesn't exist in Zookeeper. This processing about setting meta-data to
        Zookeeper be called *register*.

        * Activate and run new thread to keep updating heartbeat info
        If the signal *_Updating_Stop_Signal* is False, it would create a new thread here and it only keeps doing
        one thing again and again --- updating it owns heartbeat info by itself.

        * Check it's ready for running election
        There are 2 conditions to check whether it is ready or not:
            1. The size of *GroupState.current_crawler* is equal to the sum of runner and backup.
            2. Current crawler instance's name be included in the list *GroupState.current_crawler*.
        The checking is forever (option *timeout* is -1) and it only waits 0.5 seconds between every checks.

        * Run election for runner
        Elect which ones are runner and rest ones are backup. It could exchange different strategies to let election has
        different win conditions.

        * Update the role of crawler instance
        After election, everyone must know what role it is, and they would update this info to Zookeeper to prepare for
        doing their own jobs.

        Returns:
            None

        """
        self.register_metadata()
        if self._updating_stop_signal is False:
            self._run_updating_heartbeat_thread()
        if self.is_ready_for_election(interval=0.5, timeout=-1):
            if self.elect() is ElectionResult.WINNER:
                self._crawler_role = CrawlerStateRole.RUNNER
            else:
                self._crawler_role = CrawlerStateRole.BACKUP_RUNNER
            self._update_crawler_role(self._crawler_role)
        else:
            raise TimeoutError("Timeout to wait for crawler be ready in register process.")

    def register_metadata(self) -> None:
        """Register all needed meta-data (*GroupState*, *NodeState*, *Task* and *Heartbeat*) to Zookeeper.

        Returns:
            None

        """
        # Question: How could it name the web crawler with number?
        # Current Answer:
        # 1. Index: get data from Zookeeper first and check the value, and it set the next index of crawler and
        # save it to Zookeeper.
        # 2. Hardware code: Use the unique hardware code or flag to record it, i.e., the MAC address of host or
        # something ID of container.
        self.register_group_state()
        self.register_node_state()
        self.register_task()
        self.register_heartbeat(update_time=self._heartbeat_update,
                                update_timeout=self._heartbeat_update_timeout,
                                heart_rhythm_timeout=self._heartbeat_dead_threshold)

    def register_group_state(self) -> None:
        """Register meta-data *GroupState* to Zookeeper. This processing is more difficult than others because it's a
        common meta-data for all crawler instances in cluster to refer. So here processing we need to wait everyone
        sync their info to it, in other words, it runs synchronously to ensure they won't cover other's values by
        each others.

        It also has a flag *ensure_register*. If it's True, it would double confirm the meta-data would be reasonable
        by specific conditions:

        * The size of *GroupState.current_crawler* is equal to the sum of runner and backup.
        * Current crawler instance's name be included in the list *GroupState.current_crawler*.

        Returns:
            None

        """
        for _ in range(self._ensure_timeout):
            with self._zookeeper_client.restrict(path=self.group_state_zookeeper_path,
                                                 restrict=ZookeeperRecipe.WRITE_LOCK,
                                                 identifier=self._state_identifier):
                if not self._zookeeper_client.exist_node(path=self.group_state_zookeeper_path):
                    state = Initial.group_state(crawler_name=self._crawler_name,
                                                total_crawler=self._runner + self._backup,
                                                total_runner=self._runner,
                                                total_backup=self._backup)
                    self._metadata_util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path,
                                                                  metadata=state,
                                                                  create_node=True)
                else:
                    state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                            as_obj=GroupState)
                    if not state.current_crawler or self._crawler_name not in state.current_crawler:
                        state = Update.group_state(state,
                                                   total_crawler=self._runner + self._backup,
                                                   total_runner=self._runner,
                                                   total_backup=self._backup,
                                                   append_current_crawler=[self._crawler_name],
                                                   standby_id=self._initial_standby_id)
                        self._metadata_util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path,
                                                                      metadata=state)

            if not self._ensure_register:
                break

            state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                    as_obj=GroupState)
            assert state, "The meta data *State* should NOT be None."
            if len(set(state.current_crawler)) == self._total_crawler and self._crawler_name in state.current_crawler:
                break
            if self._ensure_wait:
                time.sleep(self._ensure_wait)
        else:
            raise TimeoutError(
                f"It gets timeout of registering meta data *GroupState* to Zookeeper cluster '{self.zookeeper_hosts}'.")

    def register_node_state(self) -> None:
        """Register meta-data *NodeState* to Zookeeper.

        Returns:
            None

        """
        state = Initial.node_state(group=self._crawler_group)
        create_node = not self._zookeeper_client.exist_node(path=self.node_state_zookeeper_path)
        self._metadata_util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path,
                                                      metadata=state,
                                                      create_node=create_node)

    def register_task(self) -> None:
        """Register meta-data *Task* to Zookeeper.

        Returns:
            None

        """
        task = Initial.task()
        create_node = not self._zookeeper_client.exist_node(path=self.task_zookeeper_path)
        self._metadata_util.set_metadata_to_zookeeper(path=self.task_zookeeper_path,
                                                      metadata=task,
                                                      create_node=create_node)

    def register_heartbeat(
            self,
            update_time: float = None,
            update_timeout: float = None,
            heart_rhythm_timeout: int = None,
            time_format: str = None,
    ) -> None:
        """Register meta-data *Heartbeat* to Zookeeper.

        Args:
            update_time (float): The time frequency to update heartbeat info, i.g., if value is '2', it would update
                heartbeat info every 2 seconds. The unit is seconds.
            update_timeout (float): The timeout value of updating, i.g., if value is '3', it is time out if it doesn't
                to update heartbeat info exceeds 3 seconds. The unit is seconds.
            heart_rhythm_timeout (int): The threshold of timeout times to judge it is dead, i.g., if value is '3' and
                the updating timeout exceeds 3 times, it would be marked as 'Dead_<Role>' (like 'Dead_Runner' or
                'Dead_Backup').
            time_format (str): The time format. This format rule is same as 'datetime'.

        Returns:
            None

        """
        update_time = f"{update_time}s" if update_time else None
        update_timeout = f"{update_timeout}s" if update_timeout else None
        heart_rhythm_timeout = f"{heart_rhythm_timeout}" if heart_rhythm_timeout else None
        heartbeat = Initial.heartbeat(update_time=update_time,
                                      update_timeout=update_timeout,
                                      heart_rhythm_timeout=heart_rhythm_timeout,
                                      time_format=time_format)
        create_node = not self._zookeeper_client.exist_node(path=self.heartbeat_zookeeper_path)
        self._metadata_util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path,
                                                      metadata=heartbeat,
                                                      create_node=create_node)

    def stop_update_heartbeat(self) -> None:
        """Set the flag of *_Updating_Stop_Signal* to be True.

        Returns:
            None

        """
        self._updating_stop_signal = True

    def is_ready_for_election(self, interval: float = 0.5, timeout: float = -1) -> bool:
        """Check whether it is ready to run next processing for function **elect**.

        Args:
            interval (float): How long it should wait between every check. Default value is 0.5 (unit is seconds).
            timeout (float): Waiting timeout, if it's -1 means it always doesn't time out. Default value is -1.

        Returns:
            bool: If it's True, means that it's ready to run next processing.

        """

        def _chk_by_condition(state: GroupState) -> bool:
            return len(state.current_crawler) == self._total_crawler and self._crawler_name in state.current_crawler

        return self._is_ready_by_groupstate(condition_callback=_chk_by_condition,
                                            interval=interval,
                                            timeout=timeout)

    def is_ready_for_run(self, interval: float = 0.5, timeout: float = -1) -> bool:
        """Check whether it is ready to run next processing for function **run**.

        Args:
            interval (float): How long it should wait between every check. Default value is 0.5 (unit is seconds).
            timeout (float): Waiting timeout, if it's -1 means it always doesn't time out. Default value is -1.

        Returns:
            bool: If it's True, means that it's ready to run next processing.

        """

        def _chk_by_condition(state: GroupState) -> bool:
            return len(state.current_crawler) == self._total_crawler and \
                   self._crawler_name in state.current_crawler and \
                   len(state.current_runner) == self._runner and \
                   len(state.current_backup) == self._backup

        return self._is_ready_by_groupstate(condition_callback=_chk_by_condition,
                                            interval=interval,
                                            timeout=timeout)

    def _is_ready_by_groupstate(self, condition_callback: Callable, interval: float = 0.5, timeout: float = -1) -> bool:
        """Use the *condition_callback* option to judge it is ready for next running or not. The callback function must
        have an argument is **GroupState**.

        In cluster running, it has some scenarios would need to wait for data be synced successfully and finish. And
        the scenarios in SmoothCrawler-Cluster would deeply depend on meta-data _GroupState_.

        Args:
            condition_callback (Callable): The callback function of checking and judge it's ready to run next process or
                not.
            interval (float): How long it should wait between every check. Default value is 0.5 (unit is seconds).
            timeout (float): Waiting timeout, if it's -1 means it always doesn't time out. Default value is -1.

        Returns:
            bool: If it's True, means that it's ready to run next processing.

        """
        if timeout < -1:
            raise ValueError("The option *timeout* value is incorrect. Please configure more than -1, and -1 means it "
                             "never timeout.")

        start = time.time()
        while True:
            state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                    as_obj=GroupState)
            if condition_callback(state):
                return True
            if timeout != -1:
                if (time.time() - start) >= timeout:
                    return False
            time.sleep(interval)

    def elect(self) -> ElectionResult:
        """Run election to choose which crawler instances are runner and rest ones are backup.

        Returns:
            A **ElectionResult** type value.

        """
        state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                as_obj=GroupState)
        return self._election_strategy.elect(candidate=self._crawler_name,
                                             member=state.current_crawler,
                                             index_sep=self._index_sep,
                                             spot=self._runner)

    def run(
            self,
            interval: float = 0.5,
            timeout: int = -1,
            wait_task_time: float = 2,
            standby_wait_time: float = 0.5,
            wait_to_be_standby_time: float = 2,
            reset_timeout_threshold: int = 10,
    ) -> None:
        """The major function of the cluster. It has a simple workflow cycle:

        <has an image of the workflow>

        Args:
            interval (float): How long it should wait between every check. Default value is 0.5 (unit is seconds).
            timeout (int): Waiting timeout, if it's -1 means it always doesn't time out. Default value is -1.
            wait_task_time (float): For a Runner, how long does the crawler instance wait a second for next task. The
                unit is seconds and default value is 2.
            standby_wait_time (float): For a Backup, how long does the crawler instance wait a second for next checking
                heartbeat. The unit is seconds and default value is 0.5.
            wait_to_be_standby_time (float): For a Backup but isn't the primary one, how long does the crawler instance
                wait a second for next checking GroupState.standby_id. The unit is seconds and default value is 2.
            reset_timeout_threshold (int): The threshold of how many straight times it doesn't occur, then it would
                reset the timeout record.

        Returns:
            None

        """
        if self.is_ready_for_run(interval=interval, timeout=timeout):
            while True:
                self.pre_running()
                try:
                    self.running_as_role(role=self._crawler_role,
                                         wait_task_time=wait_task_time,
                                         standby_wait_time=standby_wait_time,
                                         wait_to_be_standby_time=wait_to_be_standby_time,
                                         reset_timeout_threshold=reset_timeout_threshold)
                except Exception as e:
                    self.before_dead(e)
        else:
            raise TimeoutError("Timeout to wait for crawler be ready for running crawler cluster.")

    def pre_running(self) -> None:
        """Pre-processing which would be run first of all. Default implementation is doing nothing.

        Returns:
            None

        """
        pass

    def running_as_role(
            self,
            role: CrawlerStateRole,
            wait_task_time: float = 2,
            standby_wait_time: float = 0.5,
            wait_to_be_standby_time: float = 2,
            reset_timeout_threshold: int = 10,
    ) -> None:
        """Running the crawler instance's own job by what role it is.

        * _CrawlerStateRole.Runner_ ->
            waiting for tasks and run it if it gets.
        * _CrawlerStateRole.Backup_Runner_ with index is standby_id ->
            keep checking every runner's heartbeat and standby to activate to be a runner by itself if it discovers
            anyone is dead.
        * _CrawlerStateRole.Backup_Runner_ with index is not standby_id ->
            keep checking the standby ID, and to be primary backup if the standby ID is equal to its index.

        Args:
            role (CrawlerStateRole): The role of crawler instance.
            wait_task_time (float): For a Runner, how long does the crawler instance wait a second for next task. The
                unit is seconds and default value is 2.
            standby_wait_time (float): For a Backup, how long does the crawler instance wait a second for next checking
                heartbeat. The unit is seconds and default value is 0.5.
            wait_to_be_standby_time (float): For a Backup but isn't the primary one, how long does the crawler instance
                wait a second for next checking GroupState.standby_id. The unit is seconds and default value is 2.
            reset_timeout_threshold (int): The threshold of how many straight times it doesn't occur, then it would
                reset the timeout record.

        Returns:
            None

        """
        if role is CrawlerStateRole.RUNNER:
            self.wait_for_task(wait_time=wait_task_time)
        elif role is CrawlerStateRole.BACKUP_RUNNER:
            group_state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                          as_obj=GroupState)
            if self._crawler_name.split(self._index_sep)[-1] == group_state.standby_id:
                self.wait_and_standby(wait_time=standby_wait_time, reset_timeout_threshold=reset_timeout_threshold)
            else:
                self.wait_for_to_be_standby(wait_time=wait_to_be_standby_time)

    def before_dead(self, exception: Exception) -> None:
        """Do something when it gets an exception. The default implementation is raising the exception to outside.

        Args:
            exception (Exception): The exception it got.

        Returns:
            None

        """
        raise exception

    def wait_for_task(self, wait_time: float = 2) -> None:
        """Keep waiting for tasks coming and run it.

        Args:
            wait_time (float): For a Runner, how long does the crawler instance wait a second for next task. The unit is
                seconds and default value is 2.

        Returns:
            None

        """
        # 1. Try to get data from the target mode path of Zookeeper
        # 2. if (step 1 has data and it's valid) {
        #        Start to run tasks from the data
        #    } else {
        #        Keep wait for tasks
        #    }
        while True:
            task = self._metadata_util.get_metadata_from_zookeeper(path=self.task_zookeeper_path,
                                                                   as_obj=Task,
                                                                   must_has_data=False)
            if task.running_content:
                # Start to run tasks ...
                self.run_task(task=task)
            else:
                # Keep waiting
                time.sleep(wait_time)

    def wait_and_standby(self, wait_time: float = 0.5, reset_timeout_threshold: int = 10) -> None:
        """Keep checking everyone's heartbeat info, and standby to activate to be a runner by itself if it discovers
        anyone is dead.

        It would record 2 types checking result as dict type value and the data structure like below:
            * timeout records: {<crawler_name>: <timeout times>}
            * not timeout records: {<crawler_name>: <not timeout times>}

        It would record the timeout times into the value, and it would get each types timeout threshold from meta-data
        **Heartbeat**. So it would base on them to run the checking process.

        Prevent from the times be counted by unstable network environment and cause the crawler instance be marked as
        *Dead*, it won't count the times by accumulation, it would reset the timeout value if the record be counted long
        time ago. Currently, it would reset the timeout value if it was counted 10 times ago.

        Args:
            wait_time (float): For a Backup, how long does the crawler instance wait a second for next checking
                heartbeat. The unit is seconds and default value is 0.5.
            reset_timeout_threshold (int): The threshold of how many straight times it doesn't occur, then it would
                reset the timeout record.

        Returns:
            None

        """
        timeout_records: Dict[str, int] = {}
        no_timeout_records: Dict[str, int] = {}

        def _one_current_runner_is_dead(runner_name: str) -> Optional[str]:
            current_runner_heartbeat = self._metadata_util.get_metadata_from_zookeeper(
                path=f"{self._generate_path(runner_name)}/{self._zookeeper_heartbeat_node_path}",
                as_obj=Heartbeat
            )

            heart_rhythm_time = current_runner_heartbeat.heart_rhythm_time
            time_format = current_runner_heartbeat.time_format
            update_timeout = current_runner_heartbeat.update_timeout
            heart_rhythm_timeout = current_runner_heartbeat.heart_rhythm_timeout

            diff_datetime = datetime.now() - datetime.strptime(heart_rhythm_time, time_format)
            if diff_datetime.total_seconds() >= parse_timer(update_timeout):
                # It should start to pay attention on it
                timeout_records[runner_name] = timeout_records.get(runner_name, 0) + 1
                no_timeout_records[runner_name] = 0
                if timeout_records[runner_name] >= int(heart_rhythm_timeout):
                    return runner_name
            else:
                no_timeout_records[runner_name] = no_timeout_records.get(runner_name, 0) + 1
                if no_timeout_records[runner_name] >= reset_timeout_threshold:
                    timeout_records[runner_name] = 0
            return None

        while True:
            group_state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                          as_obj=GroupState)
            chk_current_runners_is_dead = map(_one_current_runner_is_dead, group_state.current_runner)
            dead_current_runner_iter = filter(lambda _dead_runner: _dead_runner is not None,
                                              list(chk_current_runners_is_dead))
            dead_current_runner = list(dead_current_runner_iter)
            if dead_current_runner:
                # Only handle the first one of dead crawlers (if it has multiple dead crawlers
                runner_name = dead_current_runner[0]
                heartbeat = self._metadata_util.get_metadata_from_zookeeper(
                    path=f"{self._generate_path(runner_name)}/{self._zookeeper_heartbeat_node_path}",
                    as_obj=Heartbeat
                )
                task_of_dead_crawler = self.discover(dead_crawler_name=runner_name, heartbeat=heartbeat)
                self.activate(dead_crawler_name=runner_name)
                self.hand_over_task(task=task_of_dead_crawler)
                break

            time.sleep(wait_time)

    def wait_for_to_be_standby(self, wait_time: float = 2) -> bool:
        """Keep waiting to be the primary backup crawler instance.

        Args:
            wait_time (float): For a Backup but isn't the primary one, how long does the crawler instance wait a second
                for next checking GroupState.standby_id. The unit is seconds and default value is 2.

        Returns:
            bool: It's True if it directs the standby ID attribute value is equal to its index of name.

        """
        while True:
            group_state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                          as_obj=GroupState)
            if self._crawler_name.split(self._index_sep)[-1] == group_state.standby_id:
                # Start to do wait_and_standby
                return True
            time.sleep(wait_time)

    def run_task(self, task: Task) -> None:
        """Run the task it directs. It runs the task by meta-data *Task.running_content* and records the running result
        back to meta-data *Task.running_result* and *Task.result_detail*.

        The core implementation of how it works web spider job in protected function *_run_crawling_processing* (and
        *processing_crawling_task*).

        Args:
            task (Task): The task it directs.

        Returns:
            None

        """
        running_content = task.running_content
        current_task: Task = task
        start_task_id = task.in_progressing_id

        assert re.search(r"[0-9]{1,32}]", start_task_id) is None, "The task index must be integer format value."

        for index, content in enumerate(running_content[int(start_task_id):]):
            content = TaskContentDataUtils.convert_to_running_content(content)

            # Update the ongoing task ID
            original_task = self._metadata_util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
            if index == 0:
                current_task = Update.task(task=original_task,
                                           in_progressing_id=content.task_id,
                                           running_status=TaskResult.PROCESSING)
            else:
                current_task = Update.task(task=original_task, in_progressing_id=content.task_id)
            self._metadata_util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=current_task)

            # Run the task and update the meta data Task
            data = None
            try:
                # TODO: Consider of here usage about how to implement to let it be more clear and convenience in usage
                #  in cient site
                data = self._run_crawling_processing(content)
            except NotImplementedError as e:
                raise e
            except Exception as e:
                # Update attributes with fail result
                running_result = TaskContentDataUtils.convert_to_running_result(original_task.running_result)
                updated_running_result = RunningResult(success_count=running_result.success_count,
                                                       fail_count=running_result.fail_count + 1)

                result_detail = original_task.result_detail
                # TODO: If it gets fail, how to record the result?
                result_detail.append(ResultDetail(task_id=content.task_id,
                                                  state=TaskResult.ERROR.value,
                                                  status_code=500,
                                                  response=None,
                                                  error_msg=f"{e}"))
            else:
                # Update attributes with successful result
                running_result = TaskContentDataUtils.convert_to_running_result(original_task.running_result)
                updated_running_result = RunningResult(success_count=running_result.success_count + 1,
                                                       fail_count=running_result.fail_count)

                result_detail = original_task.result_detail
                # TODO: Some information like HTTP status code of response should be get from response object.
                result_detail.append(ResultDetail(task_id=content.task_id,
                                                  state=TaskResult.DONE.value,
                                                  status_code=200,
                                                  response=data,
                                                  error_msg=None))

            current_task = Update.task(task=original_task,
                                       in_progressing_id=content.task_id,
                                       running_result=updated_running_result,
                                       result_detail=result_detail)
            self._metadata_util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=current_task)

        # Finish all tasks, record the running result and reset the content ...
        current_task = Update.task(task=current_task,
                                   running_content=[],
                                   in_progressing_id="-1",
                                   running_status=TaskResult.DONE)
        self._metadata_util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=current_task)

    def processing_crawling_task(self, content: RunningContent) -> Any:
        """The core of web spider implementation. All running functions it used are developers are familiar with ---
        SmoothCrawler components.

        Args:
            content (RunningContent): A RunningContent type object which provides clear attributes to run crawling task.

        Returns:
            Any: The running result of crawling.

        """
        # TODO: Consider about how to let crawler core implementation to be more scalable and flexible.
        parsed_response = self.crawl(method=content.method, url=content.url)
        data = self.data_process(parsed_response)
        # self.persist(data=_data)
        return data

    def discover(self, dead_crawler_name: str, heartbeat: Heartbeat) -> Task:
        """When backup role crawler instance discover anyone is dead, it would mark the target one as *Dead*
        (*HeartState.Asystole*) and update its meta-data **Heartbeat**. In the same time, it would try to get
        its **Task** and take over it.

        Args:
            dead_crawler_name (str): The crawler name which be under checking.
            heartbeat (Heartbeat): The meta-data **Heartbeat** of crawler be under checking.

        Returns:
            Task: Meta-data **Task** from dead crawler instance.

        """
        node_state_path = f"{self._generate_path(dead_crawler_name)}/{self._zookeeper_node_state_node_path}"
        node_state = self._metadata_util.get_metadata_from_zookeeper(path=node_state_path, as_obj=NodeState)
        node_state.role = CrawlerStateRole.DEAD_RUNNER
        self._metadata_util.set_metadata_to_zookeeper(path=node_state_path, metadata=node_state)

        task = self._metadata_util.get_metadata_from_zookeeper(
            path=f"{self._generate_path(dead_crawler_name)}/{self._zookeeper_task_node_path}",
            as_obj=Task)
        heartbeat.healthy_state = HeartState.ASYSTOLE
        heartbeat.task_state = task.running_status
        self._metadata_util.set_metadata_to_zookeeper(
            path=f"{self._generate_path(dead_crawler_name)}/{self._zookeeper_heartbeat_node_path}",
            metadata=heartbeat)

        return task

    def activate(self, dead_crawler_name: str) -> None:
        """After backup role crawler instance marks target as *Dead*, it would start to activate to be running by itself
        and run the runner's job.

        Args:
            dead_crawler_name (str): The crawler name which be under checking.

        Returns:
            None

        """
        node_state = self._metadata_util.get_metadata_from_zookeeper(path=self.node_state_zookeeper_path,
                                                                     as_obj=NodeState)
        self._crawler_role = CrawlerStateRole.RUNNER
        node_state.role = self._crawler_role
        self._metadata_util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=node_state)

        with self._zookeeper_client.restrict(path=self.group_state_zookeeper_path,
                                             restrict=ZookeeperRecipe.WRITE_LOCK,
                                             identifier=self._state_identifier):
            state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                    as_obj=GroupState)

            state.total_backup = state.total_backup - 1
            state.current_crawler.remove(dead_crawler_name)
            state.current_runner.remove(dead_crawler_name)
            state.current_runner.append(self._crawler_name)
            state.current_backup.remove(self._crawler_name)
            state.fail_crawler.append(dead_crawler_name)
            state.fail_runner.append(dead_crawler_name)
            state.standby_id = str(int(state.standby_id) + 1)

            self._metadata_util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=state)

    def hand_over_task(self, task: Task) -> None:
        """Hand over the task of the dead crawler instance. It would get the meta-data **Task** from dead crawler and
        write it to this crawler's meta-data **Task**.

        Args:
            task (Task): The meta-data **Task** of crawler be under checking.

        Returns:
            None

        """
        if task.running_status == TaskResult.PROCESSING.value:
            # Run the tasks from the start index
            self._metadata_util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=task)
        elif task.running_status in [TaskResult.NOTHING.value, TaskResult.ERROR.value]:
            # Reset some specific attributes
            updated_task = Update.task(task,
                                       in_progressing_id="0",
                                       running_result=RunningResult(success_count=0, fail_count=0),
                                       result_detail=[])
            # Reruns all tasks
            self._metadata_util.set_metadata_to_zookeeper(path=self.task_zookeeper_path, metadata=updated_task)
        else:
            # Ignore and don't do anything if the task state is nothing or done.
            pass

    def _update_crawler_role(self, role: CrawlerStateRole) -> None:
        """Update to be what role current crawler instance is in crawler cluster.

        Args:
            role (CrawlerStateRole): The role of crawler instance.

        Returns:
            None

        """
        node_state = self._metadata_util.get_metadata_from_zookeeper(path=self.node_state_zookeeper_path,
                                                                     as_obj=NodeState)
        updated_node_state = Update.node_state(node_state=node_state, role=role)
        self._metadata_util.set_metadata_to_zookeeper(path=self.node_state_zookeeper_path, metadata=updated_node_state)

        with self._zookeeper_client.restrict(path=self.group_state_zookeeper_path,
                                             restrict=ZookeeperRecipe.WRITE_LOCK,
                                             identifier=self._state_identifier):
            state = self._metadata_util.get_metadata_from_zookeeper(path=self.group_state_zookeeper_path,
                                                                    as_obj=GroupState)
            if role is CrawlerStateRole.RUNNER:
                updated_state = Update.group_state(state, append_current_runner=[self._crawler_name])
            elif role is CrawlerStateRole.BACKUP_RUNNER:
                crawler_index = self._crawler_name.split(self._index_sep)[-1]
                current_standby_id = state.standby_id
                if int(crawler_index) > int(current_standby_id) and current_standby_id != self._initial_standby_id:
                    updated_state = Update.group_state(state, append_current_backup=[self._crawler_name])
                else:
                    updated_state = Update.group_state(state,
                                                       append_current_backup=[self._crawler_name],
                                                       standby_id=self._crawler_name.split(self._index_sep)[-1])
            else:
                raise ValueError(f"It doesn't support {role} recently.")

            self._metadata_util.set_metadata_to_zookeeper(path=self.group_state_zookeeper_path, metadata=updated_state)

    def _run_updating_heartbeat_thread(self) -> None:
        """Activate and run a new thread to keep updating **Heartbeat** info.

        Returns:
            None

        """
        updating_heartbeat_thread = threading.Thread(target=self._update_heartbeat)
        updating_heartbeat_thread.daemon = True
        updating_heartbeat_thread.start()

    def _update_heartbeat(self) -> None:
        """The main function of updating **Heartbeat** info.

        .. note::
            It has a flag *_Updating_Exception*. If it's True, it would stop updating **Heartbeat**.

        Returns:
            None

        """
        while True:
            if not self._updating_stop_signal:
                try:
                    # Get *Task* and *Heartbeat* info
                    task = self._metadata_util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                    heartbeat = self._metadata_util.get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path,
                                                                                as_obj=Heartbeat)

                    # Update the values
                    heartbeat = Update.heartbeat(heartbeat,
                                                 heart_rhythm_time=datetime.now(),
                                                 healthy_state=HeartState.HEALTHY,
                                                 task_state=task.running_status)
                    self._metadata_util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path,
                                                                  metadata=heartbeat)

                    # Sleep ...
                    time.sleep(parse_timer(heartbeat.update_time))
                except Exception as e:
                    self._updating_exception = e
                    break
            else:
                task = self._metadata_util.get_metadata_from_zookeeper(path=self.task_zookeeper_path, as_obj=Task)
                heartbeat = self._metadata_util.get_metadata_from_zookeeper(path=self.heartbeat_zookeeper_path,
                                                                            as_obj=Heartbeat)
                heartbeat = Update.heartbeat(heartbeat,
                                             heart_rhythm_time=datetime.now(),
                                             healthy_state=HeartState.APPARENT_DEATH,
                                             task_state=task.running_status)
                self._metadata_util.set_metadata_to_zookeeper(path=self.heartbeat_zookeeper_path, metadata=heartbeat)
                break
        if self._updating_exception:
            raise self._updating_exception

    def _run_crawling_processing(self, content: RunningContent) -> Any:
        """The wrapper function of core crawling function *processing_crawling_task*. This meaning is checking the
        SmoothCrawler components have been registered or not.

        Args:
            content (RunningContent): A RunningContent type object which provides clear attributes to run crawling task.

        Returns:
            Any: The running result of crawling.

        """
        if self._chk_register(persist=False):
            data = self.processing_crawling_task(content)
            return data
        else:
            raise NotImplementedError("You should implement the SmoothCrawler components and register them.")

    def _chk_register(
            self,
            http_sender: bool = True,
            http_resp_parser: bool = True,
            data_hdler: bool = True,
            persist: bool = True,
    ) -> bool:
        """Checking the SmoothCrawler components has been registered or not.

        Args:
            http_sender: Checking component *CrawlerFactory.http_factory* if it's True.
            http_resp_parser: Checking component *CrawlerFactory.parser_factory* if it's True.
            data_hdler: Checking component *CrawlerFactory.data_handling_factory* if it's True.
            persist: Checking component *CrawlerFactory.data_handling_factory* if it's True.

        Returns:
            bool: If it's True, means SmoothCrawler have been registered.

        """
        def _is_not_none(chk: bool, val) -> bool:
            if chk:
                return val is not None
            return True

        factory = self._factory
        return _is_not_none(http_sender, factory.http_factory) and \
               _is_not_none(http_resp_parser, factory.parser_factory) and \
               _is_not_none(data_hdler, factory.data_handling_factory) and \
               _is_not_none(persist, factory.data_handling_factory)
