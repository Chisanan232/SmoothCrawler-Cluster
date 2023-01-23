"""*Model: data classes or enum objects for package*

In a crawler cluster/decentralized system or distributed system, it must have a lot of meta-data objects to transfer to
each other instances to get information about state in the entire system to know what happen and what things they should
do to processing that. Here are the data classes and enum objects for recording, serialize, deserialize, etc, these
information.
"""

from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import List, Union

from .metadata import GroupState, NodeState, Task, RunningContent, RunningResult, ResultDetail, Heartbeat
from .metadata_enum import CrawlerStateRole, TaskResult, HeartState


class _BaseDataObjectUtils(metaclass=ABCMeta):
    """*The base class of different util objects*

    There are some different types of operating meta-data objects: *empty*, *initial* and *update*. So it's naming the
    operating as class name and rules every operating object should have functions for every different meta-data objects
    *GroupState*, *NodeState*, *Task* and *Heartbeat*.
    """

    @staticmethod
    @abstractmethod
    def group_state(*args, **kwargs) -> GroupState:
        """Operating for meta-data object **GroupState**.

        Args:
            *args (tuple): Pass parameters though tuple type value.
            **kwargs (dict): Pass parameters though dict type value.

        Returns:
            GroupState: The **GroupState** meta-data object.

        """
        pass

    @staticmethod
    @abstractmethod
    def node_state(*args, **kwargs) -> NodeState:
        """Operating for meta-data object **NodeState**.

        Args:
            *args (tuple): Pass parameters though tuple type value.
            **kwargs (dict): Pass parameters though dict type value.

        Returns:
            NodeState: The **NodeState** meta-data object.

        """
        pass

    @staticmethod
    @abstractmethod
    def task(*args, **kwargs) -> Task:
        """Operating for meta-data object **Task**.

        Args:
            *args (tuple): Pass parameters though tuple type value.
            **kwargs (dict): Pass parameters though dict type value.

        Returns:
            Task: The **Task** meta-data object.

        """
        pass

    @staticmethod
    @abstractmethod
    def heartbeat(*args, **kwargs) -> Heartbeat:
        """Operating for meta-data object **Heartbeat**.

        Args:
            *args (tuple): Pass parameters though tuple type value.
            **kwargs (dict): Pass parameters though dict type value.

        Returns:
            Heartbeat: The **Heartbeat** meta-data object.

        """
        pass


class Empty(_BaseDataObjectUtils):
    """*Empty meta-data objects*

    Generate an empty meta-data objects without any values.
    """

    @staticmethod
    def group_state() -> GroupState:
        """Generate an empty meta-data object **GroupState**.

        Returns:
            GroupState: An empty **GroupState** meta-data object.

        """
        group_state = GroupState()
        group_state.total_crawler = 0
        group_state.total_runner = 0
        group_state.total_backup = 0
        group_state.current_crawler = []
        group_state.current_runner = []
        group_state.current_backup = []
        group_state.standby_id = "0"
        group_state.fail_crawler = []
        group_state.fail_runner = []
        group_state.fail_backup = []
        return group_state

    @staticmethod
    def node_state() -> NodeState:
        """Generate an empty meta-data object **NodeState**.

        Returns:
            NodeState: An empty **NodeState** meta-data object.

        """
        node_state = NodeState()
        node_state.group = ""
        node_state.role = CrawlerStateRole.INITIAL
        return node_state

    @staticmethod
    def task() -> Task:
        """Generate an empty meta-data object **Task**.

        Returns:
            Task: An empty **Task** meta-data object.

        """
        task = Task()
        task.running_content = []
        task.cookie = {}
        task.authorization = {}
        task.in_progressing_id = "-1"
        task.running_result = RunningResult(success_count=0, fail_count=0)
        task.running_status = TaskResult.NOTHING
        task.result_detail = []
        return task

    @staticmethod
    def heartbeat() -> Heartbeat:
        """Generate an empty meta-data object **Heartbeat**.

        Returns:
            Heartbeat: An empty **Heartbeat** meta-data object.

        """
        heartbeat = Heartbeat()
        heartbeat.time_format = "%Y-%m-%d %H:%M:%S"
        heartbeat.heart_rhythm_time = datetime.now().strftime(heartbeat.time_format)
        heartbeat.update_time = "2s"
        heartbeat.update_timeout = "4s"
        heartbeat.heart_rhythm_timeout = "3"
        heartbeat.healthy_state = HeartState.NEWBORN
        heartbeat.task_state = TaskResult.NOTHING
        return heartbeat


class Initial(_BaseDataObjectUtils):
    """*Initialize a meta-data object with values*

    Initial meta-data object with values at one or more multiple specific different options.
    """

    @staticmethod
    def group_state(
            crawler_name: str,
            total_crawler: int,
            total_runner: int,
            total_backup: int,
            standby_id: str = "0",
            current_crawler: List[str] = [],
            current_runner: List[str] = [],
            current_backup: List[str] = [],
            fail_crawler: List[str] = [],
            fail_runner: List[str] = [],
            fail_backup: List[str] = [],
    ) -> GroupState:
        """Initialize a meta-data object **GroupState** with values.

        Args:
            crawler_name (str): Crawler instance's name.
            total_crawler (int): Total amount of crawler includes every role.
            total_runner (int): Total amount of crawler which is *Runner*.
            total_backup (int): Total amount of crawler which is *Backup_Runner*.
            standby_id (str): The standby ID. It should be the index of crawler name.
            current_crawler (list of str): A list of total crawler instance's name includes every role.
            current_runner (list of str): A list of total crawler instance's name which is *Runner*.
            current_backup (list of str): A list of total crawler instance's name which is *Backup_Runner*.
            fail_crawler (list of str): A list of total crawler instance's name which is dead state.
            fail_runner (list of str): A list of total crawler instance's name which is *Dead_Runner*.
            fail_backup (list of str): A list of total crawler instance's name which is *Dead_Backup_Runner*.

        Returns:
            GroupState: An **GroupState** meta-data object with value(s).

        """
        group_state = GroupState()
        group_state.total_crawler = total_crawler
        group_state.total_runner = total_runner
        group_state.total_backup = total_backup
        current_crawler = list(set(current_crawler))
        if not current_crawler or crawler_name not in set(current_crawler):
            current_crawler.append(crawler_name)
        group_state.current_crawler = current_crawler
        group_state.current_runner = current_runner
        group_state.current_backup = current_backup
        group_state.standby_id = standby_id
        group_state.fail_crawler = fail_crawler
        group_state.fail_runner = fail_runner
        group_state.fail_backup = fail_backup
        return group_state

    @staticmethod
    def node_state(group: str = None, role: CrawlerStateRole = None) -> NodeState:
        """Initialize a meta-data object **NodeState** with values.

        Args:
            group (str): The name of group which the current crawler instance belong to.
            role (CrawlerStateRole): The role of current crawler instance.

        Returns:
            NodeState: An **NodeState** meta-data object with value(s).

        """
        node_state = NodeState()
        if group:
            node_state.group = group
        if not role:
            role = CrawlerStateRole.INITIAL
        node_state.role = role
        return node_state

    @staticmethod
    def task(
            running_content: List[Union[dict, RunningContent]] = [],
            cookie: dict = {},
            authorization: dict = {},
            in_progressing_id: str = "-1",
            running_result: Union[dict, RunningResult] = None,
            running_state: TaskResult = None,
            result_detail: List[Union[dict, ResultDetail]] = [],
    ) -> Task:
        """Initialize a meta-data object **Task** with values.

        Args:
            running_content (List[Union[dict, RunningContent]]): The details of task content.
            cookie (dict): Cookie.
            authorization (dict): Authorization settings of HTTP request.
            in_progressing_id (str): The task ID which is in processing state.
            running_result (Union[dict, RunningResult]): The running result statistics about amount of successful and
                fail done tasks.
            running_state (TaskResult): The status of task running.
            result_detail (List[Union[dict, ResultDetail]]): The details of running result.

        Returns:
            Task: An **Task** meta-data object with value(s).

        """
        task = Task()
        task.running_content = running_content
        task.cookie = cookie
        task.authorization = authorization
        task.in_progressing_id = in_progressing_id
        if not running_result:
            running_result = RunningResult(success_count=0, fail_count=0)
        task.running_result = running_result
        if not running_state:
            running_state = TaskResult.NOTHING
        task.running_status = running_state
        task.result_detail = result_detail
        return task

    @staticmethod
    def heartbeat(
            time_format: str = None,
            update_time: str = None,
            update_timeout: str = None,
            heart_rhythm_timeout: str = None,
            healthy_state: HeartState = None,
            task_state: TaskResult = None,
    ) -> Heartbeat:
        """Initialize a meta-data object **Heartbeat** with values.

        Args:
            time_format (str): The format of datetime value.
            update_time (str): The timer for updating heartbeat.
            update_timeout (str): The timeout threshold of updating.
            heart_rhythm_timeout (str): The timeout threshold of entire updating process.
            healthy_state (HeartState): Heartbeat status.
            task_state (TaskResult): Task running status.

        Returns:
            Heartbeat: An **Heartbeat** meta-data object with value(s).

        """
        heartbeat = Heartbeat()
        if not time_format:
            time_format = "%Y-%m-%d %H:%M:%S"
        heartbeat.time_format = time_format
        heartbeat.heart_rhythm_time = datetime.now().strftime(time_format)
        if not update_time:
            update_time = "1s"
        heartbeat.update_time = update_time
        if not update_timeout:
            update_timeout = "2s"
        heartbeat.update_timeout = update_timeout
        if not heart_rhythm_timeout:
            heart_rhythm_timeout = "3"
        heartbeat.heart_rhythm_timeout = heart_rhythm_timeout
        if not healthy_state:
            healthy_state = HeartState.NEWBORN
        heartbeat.healthy_state = healthy_state
        if not task_state:
            task_state = TaskResult.NOTHING
        heartbeat.task_state = task_state
        return heartbeat


class Update(_BaseDataObjectUtils):
    """*Updating a meta-data object with values*

    Update the meta-data object with one or more multiple options.
    """

    @staticmethod
    def group_state(
            state: GroupState,
            total_crawler: int = None,
            total_runner: int = None,
            total_backup: int = None,
            standby_id: str = None,
            append_current_crawler: List[str] = [],
            append_current_runner: List[str] = [],
            append_current_backup: List[str] = [],
            append_fail_crawler: List[str] = [],
            append_fail_runner: List[str] = [],
            append_fail_backup: List[str] = [],
    ) -> GroupState:
        """Updating a meta-data object **GroupState** with values.

        .. note::

            The updating of some options which is list type would update value though appending element(s) at the
            current list value in Zookeeper and assigning it at the target option.

        Args:
            state (GroupState): Current *GroupState* meta-data object.
            total_crawler (int): Total amount of crawler includes every role.
            total_runner (int): Total amount of crawler which is *Runner*.
            total_backup (int): Total amount of crawler which is *Backup_Runner*.
            standby_id (str): The standby ID. It should be the index of crawler name.
            append_current_crawler (list of str): A list of total crawler instance's name includes every role.
            append_current_runner (list of str): A list of total crawler instance's name which is *Runner*.
            append_current_backup (list of str): A list of total crawler instance's name which is *Backup_Runner*.
            append_fail_crawler (list of str): A list of total crawler instance's name which is dead state.
            append_fail_runner (list of str): A list of total crawler instance's name which is *Dead_Runner*.
            append_fail_backup (list of str): A list of total crawler instance's name which is *Dead_Backup_Runner*.

        Returns:
            GroupState: An **GroupState** meta-data object with value(s).

        """
        Update._update_ele_if_not_none(data_obj=state, prop="total_crawler", new_val=total_crawler)
        Update._update_ele_if_not_none(data_obj=state, prop="total_runner", new_val=total_runner)
        Update._update_ele_if_not_none(data_obj=state, prop="total_backup", new_val=total_backup)

        Update._append_ele_if_not_none(data_obj=state, prop="current_crawler", new_val=append_current_crawler)
        Update._append_ele_if_not_none(data_obj=state, prop="current_runner", new_val=append_current_runner)
        Update._append_ele_if_not_none(data_obj=state, prop="current_backup", new_val=append_current_backup)

        Update._update_ele_if_not_none(data_obj=state, prop="standby_id", new_val=standby_id)

        Update._append_ele_if_not_none(data_obj=state, prop="fail_crawler", new_val=append_fail_crawler)
        Update._append_ele_if_not_none(data_obj=state, prop="fail_runner", new_val=append_fail_runner)
        Update._append_ele_if_not_none(data_obj=state, prop="fail_backup", new_val=append_fail_backup)

        return state

    @staticmethod
    def node_state(node_state: NodeState, group: str = None, role: CrawlerStateRole = None) -> NodeState:
        """Updating a meta-data object **NodeState** with values.

        Args:
            node_state (NodeState): Current *NodeState* meta-data object.
            group (str): The name of group which the current crawler instance belong to.
            role (CrawlerStateRole): The role of current crawler instance.

        Returns:
            NodeState: An **NodeState** meta-data object with value(s).

        """
        Update._update_ele_if_not_none(data_obj=node_state, prop="group", new_val=group)
        Update._update_ele_if_not_none(data_obj=node_state, prop="role", new_val=role)
        return node_state

    @staticmethod
    def task(
            task: Task,
            running_content: List[Union[dict, RunningContent]] = None,
            cookie: dict = None,
            authorization: dict = None,
            in_progressing_id: str = None,
            running_result: Union[dict, RunningResult] = None,
            running_status: TaskResult = None,
            result_detail: List[Union[dict, ResultDetail]] = None,
    ) -> Task:
        """Updating a meta-data object **Task** with values.

        Args:
            task (Task): Current *Task* meta-data object.
            running_content (List[Union[dict, RunningContent]]): The details of task content.
            cookie (dict): Cookie.
            authorization (dict): Authorization settings of HTTP request.
            in_progressing_id (str): The task ID which is in processing state.
            running_result (Union[dict, RunningResult]): The running result statistics about amount of successful and
                fail done tasks.
            running_status (TaskResult): The status of task running.
            result_detail (List[Union[dict, ResultDetail]]): The details of running result.

        Returns:
            Task: An **Task** meta-data object with value(s).

        """
        Update._update_ele_if_not_none(data_obj=task, prop="running_content", new_val=running_content)
        Update._update_ele_if_not_none(data_obj=task, prop="cookie", new_val=cookie)
        Update._update_ele_if_not_none(data_obj=task, prop="authorization", new_val=authorization)
        Update._update_ele_if_not_none(data_obj=task, prop="in_progressing_id", new_val=in_progressing_id)
        Update._update_ele_if_not_none(data_obj=task, prop="running_result", new_val=running_result)
        Update._update_ele_if_not_none(data_obj=task, prop="running_status", new_val=running_status)
        Update._update_ele_if_not_none(data_obj=task, prop="result_detail", new_val=result_detail)
        return task

    @staticmethod
    def heartbeat(
            heartbeat: Heartbeat,
            heart_rhythm_time: datetime = None,
            time_format: str = None,
            update_time: str = None,
            update_timeout: str = None,
            heart_rhythm_timeout: str = None,
            healthy_state: HeartState = None,
            task_state: Union[str, TaskResult] = None,
    ) -> Heartbeat:
        """Updating a meta-data object **Heartbeat** with values.

        Args:
            heartbeat (Heartbeat): Current *Heartbeat* meta-data object.
            heart_rhythm_time (datetime): It should be a *datetime.datetime* type object.
            time_format (str): The format of datetime value.
            update_time (str): The timer for updating heartbeat.
            update_timeout (str): The timeout threshold of updating.
            heart_rhythm_timeout (str): The timeout threshold of entire updating process.
            healthy_state (HeartState): Heartbeat status.
            task_state (TaskResult): Task running status.

        Returns:
            Heartbeat: An **Heartbeat** meta-data object with value(s).

        """
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="heart_rhythm_time", new_val=heart_rhythm_time)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="time_format", new_val=time_format)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="update_time", new_val=update_time)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="update_timeout", new_val=update_timeout)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="heart_rhythm_timeout", new_val=heart_rhythm_timeout)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="healthy_state", new_val=healthy_state)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="task_state", new_val=task_state)
        return heartbeat

    @staticmethod
    def _update_ele_if_not_none(
            data_obj,
            prop: str,
            new_val: Union[int, str, list, dict, datetime, CrawlerStateRole, TaskResult, HeartState],
    ) -> None:
        if new_val is not None:
            setattr(data_obj, prop, new_val)

    @staticmethod
    def _append_ele_if_not_none(data_obj, prop: str, new_val: List[str]):
        if new_val is not None:
            prop_value = getattr(data_obj, prop)
            if prop_value is None:
                prop_value = []
            prop_value += list(set(new_val))
            setattr(data_obj, prop, prop_value)
