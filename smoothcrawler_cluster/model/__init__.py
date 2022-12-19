"""Module
TODO: Add docstring
"""

from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import List, Union

from .metadata import GroupState, NodeState, Task, RunningContent, RunningResult, ResultDetail, Heartbeat
from .metadata_enum import CrawlerStateRole, TaskResult, HeartState


class _BaseDataObjectUtils(metaclass=ABCMeta):
    """Class
    TODO: Add docstring
    """

    @staticmethod
    @abstractmethod
    def group_state(*args, **kwargs) -> GroupState:
        """Function
        TODO: Add docstring
        """
        pass

    @staticmethod
    @abstractmethod
    def node_state(*args, **kwargs) -> NodeState:
        """Function
        TODO: Add docstring
        """
        pass

    @staticmethod
    @abstractmethod
    def task(*args, **kwargs) -> Task:
        """Function
        TODO: Add docstring
        """
        pass

    @staticmethod
    @abstractmethod
    def heartbeat(*args, **kwargs) -> Heartbeat:
        """Function
        TODO: Add docstring
        """
        pass


class Empty(_BaseDataObjectUtils):
    """
    Create data object you need with empty values.
    """

    @staticmethod
    def group_state() -> GroupState:
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
        node_state = NodeState()
        node_state.group = ""
        node_state.role = CrawlerStateRole.INITIAL
        return node_state

    @staticmethod
    def task() -> Task:
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
    """
    Initial data object with every different options.
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
    """
    Update the data object with every different options.
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
