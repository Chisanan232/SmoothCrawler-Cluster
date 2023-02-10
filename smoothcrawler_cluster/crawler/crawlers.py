"""*Different types cluster crawler with SmoothCrawler*

It integrates the features of **SmoothCrawler** into all the cluster crawler. So it has the features of
**SmoothCrawler**, and it also could extend the features to run in a cluster right now!
"""

import threading
import time
from abc import ABC, ABCMeta, abstractmethod
from typing import Any, Callable, Generic, List, Optional, Type, TypeVar, Union

from smoothcrawler.crawler import BaseCrawler
from smoothcrawler.factory import BaseFactory

from .._utils import MetaDataUtil
from .._utils.converter import BaseConverter, JsonStrConverter
from .._utils.zookeeper import ZookeeperClient, ZookeeperPath, ZookeeperRecipe
from ..election import BaseElection, ElectionResult, IndexElection
from ..exceptions import StopUpdateHeartbeat
from ..model import (
    CrawlerStateRole,
    GroupState,
    Initial,
    NodeState,
    RunningContent,
    Update,
)
from ..model._data import CrawlerTimer, TimeInterval, TimerThreshold
from ..model.metadata import _BaseMetaData
from .adapter import DistributedLock
from .attributes import BaseCrawlerAttribute, SerialCrawlerAttribute
from .dispatcher import WorkflowDispatcher
from .workflow import HeartbeatUpdatingWorkflow

_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)
BaseElectionType = TypeVar("BaseElectionType", bound=BaseElection)


class BaseDistributedCrawler(metaclass=ABCMeta):
    """*Base class for distributed crawler*

    The base class about distributed crawler definitions. Distributed system has many practices, i.e., using application
    integration concern to design to handle complex business logic in a large system; using cluster concern to build
    a crawler system which has high fault tolerance to support each other's feature, etc. Therefore, this is the
    most basically class for defining a distributed crawler.
    """

    pass


class BaseClusterCrawler(BaseDistributedCrawler):
    """*Base class for cluster crawler*

    The base class about cluster crawler definitions. This is the most basically class for the cluster crawler which has
    high fault tolerance feature.

    In cluster realm, it could roughly divide to 2 types: **Centralized** and **Decentralized**. The further one has
    leader role member(s) and the letter one doesn't. So it has one more sub-abstract classes of this one are
    **BaseCentralizedCrawler** and **BaseDecentralizedCrawler**.

    In generally, each of single instance in cluster all are a single one which could be standby for each other and hand
    over other's jobs if it needs. The consideration is: how could it work? what roles or jobs it need? We will have
    discussion of it with the 2 abstract classes: **BaseCentralizedCrawler** and **BaseDecentralizedCrawler**.
    """

    @abstractmethod
    def _get_metadata(
        self, path: str, as_obj: Type[_BaseMetaDataType], must_has_data: bool = True
    ) -> Generic[_BaseMetaDataType]:
        pass

    @abstractmethod
    def _set_metadata(self, path: str, metadata: Generic[_BaseMetaDataType], create_node: bool = False) -> None:
        pass


class BaseCentralizedCrawler(BaseClusterCrawler, ABC):
    """*Base class for centralized cluster crawler*

    The base class about centralized crawler definitions. For centralized system, it has leader (or be called as master
    or something else like that) role in it.

    Please refer to :ref:`Has_Leader_usage_guide` to get more details.
    """

    pass


class BaseDecentralizedCrawler(BaseClusterCrawler, ABC):
    """*Base class for decentralized cluster crawler*

    The base class about decentralized crawler definitions. For decentralized system, it doesn't have leader, master or
    something else like that role in it.

    Please refer to :ref:`No_Leader_usage_guide` to get more details.
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

    _workflow_dispatcher: WorkflowDispatcher = None
    _heartbeat_workflow: HeartbeatUpdatingWorkflow = None

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
        attribute: Optional[BaseCrawlerAttribute] = None,
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
            attribute (BaseCrawlerAttribute): The attribute type of crawler. Default strategy is
                **SerialCrawlerAttribute**.
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

        if not attribute:
            attribute = SerialCrawlerAttribute()
        self._crawler_attr = attribute
        self._crawler_attr.init(name=name, id_separation=index_sep)

        self._crawler_name = self._crawler_attr.name
        self._index_sep = self._crawler_attr.id_separation

        if not group:
            group = "sc-crawler-cluster"
        self._crawler_group = group

        self._state_identifier = "sc-crawler-cluster_state"

        if not zk_hosts:
            zk_hosts = self._default_zookeeper_hosts
        self._zk_hosts = zk_hosts
        self._zookeeper_client = ZookeeperClient(hosts=self._zk_hosts)
        self._zk_path = ZookeeperPath(name=self._crawler_name, group=self._crawler_group)

        if not zk_converter:
            zk_converter = JsonStrConverter()
        self._zk_converter = zk_converter

        self._metadata_util = MetaDataUtil(client=self._zookeeper_client, converter=self._zk_converter)

        if not election_strategy:
            election_strategy = IndexElection()
        self._election_strategy = election_strategy

        self._heartbeat_update = heartbeat_update
        self._heartbeat_update_timeout = heartbeat_update_timeout
        self._heartbeat_dead_threshold = heartbeat_dead_threshold

        restrict_args = {
            "path": self._zk_path.group_state,
            "restrict": ZookeeperRecipe.WRITE_LOCK,
            "identifier": self._state_identifier,
        }
        self._workflow_dispatcher = WorkflowDispatcher(
            crawler_name=self._crawler_name,
            group=self._crawler_group,
            index_sep=self._index_sep,
            path=self._zk_path,
            get_metadata_callback=self._get_metadata,
            set_metadata_callback=self._set_metadata,
            opt_metadata_with_lock=DistributedLock(lock=self._zookeeper_client.restrict, **restrict_args),
            crawler_process_callback=self._run_crawling_processing,
        )

        self._heartbeat_workflow = self._workflow_dispatcher.heartbeat()

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
        """:obj:`str`: Properties with both a getter and setter. The getter and setter of option *ensure_initial*."""
        return self._ensure_register

    @ensure_register.setter
    def ensure_register(self, ensure: bool) -> None:
        self._ensure_register = ensure

    @property
    def ensure_timeout(self) -> int:
        """:obj:`str`: Properties with both a getter and setter. The getter and setter of option *ensure_timeout*."""
        return self._ensure_timeout

    @ensure_timeout.setter
    def ensure_timeout(self, timeout: float) -> None:
        self._ensure_timeout = timeout

    @property
    def ensure_wait(self) -> float:
        """:obj:`str`: Properties with both a getter and setter. The getter and setter of option *ensure_wait*."""
        return self._ensure_wait

    @ensure_wait.setter
    def ensure_wait(self, wait: int) -> None:
        self._ensure_wait = wait

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
        # TODO (register): The registering process should be one single process in another object may be call it as
        #  *register* and let it to manage these processes.
        self.register_metadata()
        if self._heartbeat_workflow.stop_heartbeat is False:
            self._run_updating_heartbeat_thread()
        # TODO (crawler): It's possible that it needs to parameterize this election running workflow
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
        # TODO (election): The registering meta-data GroupState process should be one process of *election* and let it
        #  to control.
        self.register_group_state()
        # TODO (register): Parameterize the initial value of meta-data, default value should be None and let this
        #  package to help developers set.
        self.register_node_state()
        self.register_task()
        self.register_heartbeat(
            update_time=self._heartbeat_update,
            update_timeout=self._heartbeat_update_timeout,
            heart_rhythm_timeout=self._heartbeat_dead_threshold,
        )

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
            with self._zookeeper_client.restrict(
                path=self._zk_path.group_state,
                restrict=ZookeeperRecipe.WRITE_LOCK,
                identifier=self._state_identifier,
            ):
                if not self._zookeeper_client.exist_node(path=self._zk_path.group_state):
                    state = Initial.group_state(
                        crawler_name=self._crawler_name,
                        total_crawler=self._runner + self._backup,
                        total_runner=self._runner,
                        total_backup=self._backup,
                    )
                    self._set_metadata(path=self._zk_path.group_state, metadata=state, create_node=True)
                else:
                    state = self._get_metadata(path=self._zk_path.group_state, as_obj=GroupState)
                    if not state.current_crawler or self._crawler_name not in state.current_crawler:
                        state = Update.group_state(
                            state,
                            total_crawler=self._runner + self._backup,
                            total_runner=self._runner,
                            total_backup=self._backup,
                            append_current_crawler=[self._crawler_name],
                            standby_id=self._initial_standby_id,
                        )
                        self._set_metadata(path=self._zk_path.group_state, metadata=state)

            if not self._ensure_register:
                break

            state = self._get_metadata(path=self._zk_path.group_state, as_obj=GroupState)
            assert state, "The meta data *State* should NOT be None."
            if len(set(state.current_crawler)) == self._total_crawler and self._crawler_name in state.current_crawler:
                break
            if self._ensure_wait:
                time.sleep(self._ensure_wait)
        else:
            raise TimeoutError(
                f"It gets timeout of registering meta data *GroupState* to Zookeeper cluster '{self.zookeeper_hosts}'."
            )

    def register_node_state(self) -> None:
        """Register meta-data *NodeState* to Zookeeper.

        Returns:
            None

        """
        state = Initial.node_state(group=self._crawler_group)
        create_node = not self._zookeeper_client.exist_node(path=self._zk_path.node_state)
        self._set_metadata(path=self._zk_path.node_state, metadata=state, create_node=create_node)

    def register_task(self) -> None:
        """Register meta-data *Task* to Zookeeper.

        Returns:
            None

        """
        task = Initial.task()
        create_node = not self._zookeeper_client.exist_node(path=self._zk_path.task)
        self._set_metadata(path=self._zk_path.task, metadata=task, create_node=create_node)

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
        heartbeat = Initial.heartbeat(
            update_time=update_time,
            update_timeout=update_timeout,
            heart_rhythm_timeout=heart_rhythm_timeout,
            time_format=time_format,
        )
        create_node = not self._zookeeper_client.exist_node(path=self._zk_path.heartbeat)
        self._set_metadata(path=self._zk_path.heartbeat, metadata=heartbeat, create_node=create_node)

    def stop_update_heartbeat(self) -> None:
        """Set the flag of *_Updating_Stop_Signal* to be True.

        Returns:
            None

        """
        self._heartbeat_workflow.stop_heartbeat = True

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

        return self._is_ready_by_groupstate(condition_callback=_chk_by_condition, interval=interval, timeout=timeout)

    def is_ready_for_run(self, interval: float = 0.5, timeout: float = -1) -> bool:
        """Check whether it is ready to run next processing for function **run**.

        Args:
            interval (float): How long it should wait between every check. Default value is 0.5 (unit is seconds).
            timeout (float): Waiting timeout, if it's -1 means it always doesn't time out. Default value is -1.

        Returns:
            bool: If it's True, means that it's ready to run next processing.

        """

        def _chk_by_condition(state: GroupState) -> bool:
            return (
                len(state.current_crawler) == self._total_crawler
                and self._crawler_name in state.current_crawler
                and len(state.current_runner) == self._runner
                and len(state.current_backup) == self._backup
            )

        return self._is_ready_by_groupstate(condition_callback=_chk_by_condition, interval=interval, timeout=timeout)

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
            raise ValueError(
                "The option *timeout* value is incorrect. Please configure more than -1, and -1 means it "
                "never timeout."
            )

        start = time.time()
        while True:
            state = self._get_metadata(path=self._zk_path.group_state, as_obj=GroupState)
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
        state = self._get_metadata(path=self._zk_path.group_state, as_obj=GroupState)
        return self._election_strategy.elect(
            candidate=self._crawler_name, member=state.current_crawler, index_sep=self._index_sep, spot=self._runner
        )

    def run(
        self,
        interval: float = 0.5,
        timeout: int = -1,
        wait_task_time: float = 2,
        standby_wait_time: float = 0.5,
        wait_to_be_standby_time: float = 2,
        reset_timeout_threshold: int = 10,
        unlimited: bool = True,
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
            unlimited (bool): If it is *True*, this function would keep running as unlimited loop, nor it would only run
                once time.

        Returns:
            None

        Raises:
            CrawlerIsDeadError: The current crawler instance is dead.

        """
        if self.is_ready_for_run(interval=interval, timeout=timeout):
            while True:
                self.pre_running()
                try:
                    node_state = self._get_metadata(
                        path=self._zk_path.node_state, as_obj=NodeState, must_has_data=False
                    )
                    self.running_as_role(
                        role=node_state.role,
                        wait_task_time=wait_task_time,
                        standby_wait_time=standby_wait_time,
                        wait_to_be_standby_time=wait_to_be_standby_time,
                        reset_timeout_threshold=reset_timeout_threshold,
                    )
                    if unlimited is False:
                        break
                except Exception as e:  # pylint: disable=broad-except
                    self.before_dead(e)
                    break
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
        role: Union[str, CrawlerStateRole],
        wait_task_time: float = 2,
        standby_wait_time: float = 0.5,
        wait_to_be_standby_time: float = 2,
        reset_timeout_threshold: int = 10,
    ) -> Optional[bool]:
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

        Raises:
            CrawlerIsDeadError: The current crawler instance is dead.

        """
        interval = TimeInterval()
        interval.check_task = wait_task_time
        interval.check_crawler_state = standby_wait_time
        interval.check_standby_id = wait_to_be_standby_time

        threshold = TimerThreshold()
        threshold.reset_timeout = reset_timeout_threshold

        timer = CrawlerTimer()
        timer.time_interval = interval
        timer.threshold = threshold

        try:
            role = role.value if isinstance(role, CrawlerStateRole) else role
            return self._workflow_dispatcher.dispatch(role=role).run(timer=timer)
        except StopUpdateHeartbeat:
            self.stop_update_heartbeat()

    def before_dead(self, exception: Exception) -> None:
        """Do something when it gets an exception. The default implementation is raising the exception to outside.

        Args:
            exception (Exception): The exception it got.

        Returns:
            None

        """
        raise exception

    def processing_crawling_task(self, content: RunningContent) -> Any:
        """The core of web spider implementation. All running functions it used are developers are familiar with ---
        SmoothCrawler components.

        Args:
            content (RunningContent): A RunningContent type object which provides clear attributes to run crawling task.

        Returns:
            Any: The running result of crawling.

        """
        # TODO (adapter): Consider about how to let crawler core implementation to be more scalable and flexible.
        parsed_response = self.crawl(method=content.method, url=content.url)
        data = self.data_process(parsed_response)
        # self.persist(data=_data)
        return data

    def _update_crawler_role(self, role: CrawlerStateRole) -> None:
        """Update to be what role current crawler instance is in crawler cluster.

        Args:
            role (CrawlerStateRole): The role of crawler instance.

        Returns:
            None

        """
        node_state = self._get_metadata(path=self._zk_path.node_state, as_obj=NodeState)
        updated_node_state = Update.node_state(node_state=node_state, role=role)
        self._set_metadata(path=self._zk_path.node_state, metadata=updated_node_state)

        with self._zookeeper_client.restrict(
            path=self._zk_path.group_state,
            restrict=ZookeeperRecipe.WRITE_LOCK,
            identifier=self._state_identifier,
        ):
            state = self._get_metadata(path=self._zk_path.group_state, as_obj=GroupState)
            if role is CrawlerStateRole.RUNNER:
                updated_state = Update.group_state(state, append_current_runner=[self._crawler_name])
            elif role is CrawlerStateRole.BACKUP_RUNNER:
                crawler_index = self._crawler_name.split(self._index_sep)[-1]
                current_standby_id = state.standby_id
                if int(crawler_index) > int(current_standby_id) and current_standby_id != self._initial_standby_id:
                    updated_state = Update.group_state(state, append_current_backup=[self._crawler_name])
                else:
                    updated_state = Update.group_state(
                        state,
                        append_current_backup=[self._crawler_name],
                        standby_id=self._crawler_name.split(self._index_sep)[-1],
                    )
            else:
                raise ValueError(f"It doesn't support {role} recently.")

            self._set_metadata(path=self._zk_path.group_state, metadata=updated_state)

    def _run_updating_heartbeat_thread(self) -> None:
        """Activate and run a new thread to keep updating **Heartbeat** info.

        Returns:
            None

        """
        updating_heartbeat_thread = threading.Thread(target=self._heartbeat_workflow.run)
        updating_heartbeat_thread.daemon = True
        updating_heartbeat_thread.start()

    def _get_metadata(
        self, path: str, as_obj: Type[_BaseMetaDataType], must_has_data: bool = True
    ) -> Generic[_BaseMetaDataType]:
        # TODO (_utility): Let the usage could be followed as bellow python code:
        # example:
        # self._metadata_util.get(must_has_data=False).from_zookeeper(path=self._zk_path.node_state).to(NodeState)
        return self._metadata_util.get_metadata_from_zookeeper(path=path, as_obj=as_obj, must_has_data=must_has_data)

    def _set_metadata(self, path: str, metadata: Generic[_BaseMetaDataType], create_node: bool = False) -> None:
        # TODO (_utility): Let the usage could be followed as bellow python code:
        # example:
        # self._metadata_util.set(metadata=state, create_node=True).to_zookeeper(path=self._zk_path.group_state)
        self._metadata_util.set_metadata_to_zookeeper(path=path, metadata=metadata, create_node=create_node)

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
        return (
            _is_not_none(http_sender, factory.http_factory)
            and _is_not_none(http_resp_parser, factory.parser_factory)
            and _is_not_none(data_hdler, factory.data_handling_factory)
            and _is_not_none(persist, factory.data_handling_factory)
        )
