from datetime import datetime
from typing import List, Union
from abc import ABCMeta, abstractmethod

from .metadata import GroupState, NodeState, Task, Heartbeat
from .metadata_enum import CrawlerStateRole, TaskResult, HeartState


class _BaseDataObjectUtils(metaclass=ABCMeta):

    @staticmethod
    @abstractmethod
    def group_state(*args, **kwargs) -> GroupState:
        pass

    @staticmethod
    @abstractmethod
    def node_state(*args, **kwargs) -> NodeState:
        pass

    @staticmethod
    @abstractmethod
    def task(*args, **kwargs) -> Task:
        pass

    @staticmethod
    @abstractmethod
    def heartbeat(*args, **kwargs) -> Heartbeat:
        pass


class Empty(_BaseDataObjectUtils):
    """
    Create data object you need with empty values.
    """

    @staticmethod
    def group_state() -> GroupState:
        _state = GroupState()
        _state.total_crawler = 0
        _state.total_runner = 0
        _state.total_backup = 0
        _state.current_crawler = []
        _state.current_runner = []
        _state.current_backup = []
        _state.standby_id = "0"
        _state.fail_crawler = []
        _state.fail_runner = []
        _state.fail_backup = []
        return _state

    @staticmethod
    def node_state() -> NodeState:
        _node_state = NodeState()
        _node_state.group = ""
        _node_state.role = CrawlerStateRole.Initial
        return _node_state

    @staticmethod
    def task() -> Task:
        _task = Task()
        _task.task_result = TaskResult.Nothing
        _task.task_content = {}
        return _task

    @staticmethod
    def heartbeat() -> Heartbeat:
        _heartbeat = Heartbeat()
        _heartbeat.time_format = "%Y-%m-%d %H:%M:%S"
        _heartbeat.heart_rhythm_time = datetime.now().strftime(_heartbeat.time_format)
        _heartbeat.update_time = "2s"
        _heartbeat.update_timeout = "4s"
        _heartbeat.heart_rhythm_timeout = "3"
        _heartbeat.healthy_state = HeartState.Newborn
        _heartbeat.task_state = TaskResult.Nothing
        return _heartbeat


class Initial(_BaseDataObjectUtils):
    """
    Initial data object with every different options.
    """

    @staticmethod
    def group_state(crawler_name: str, total_crawler: int, total_runner: int, total_backup: int, standby_id: str = "0",
                    current_crawler: List[str] = [], current_runner: List[str] = [], current_backup: List[str] = [],
                    fail_crawler: List[str] = [], fail_runner: List[str] = [], fail_backup: List[str] = []) -> GroupState:
        _state = GroupState()
        _state.total_crawler = total_crawler
        _state.total_runner = total_runner
        _state.total_backup = total_backup
        current_crawler = list(set(current_crawler))
        if len(current_crawler) == 0 or crawler_name not in set(current_crawler):
            current_crawler.append(crawler_name)
        _state.current_crawler = current_crawler
        _state.current_runner = current_runner
        _state.current_backup = current_backup
        _state.standby_id = standby_id
        _state.fail_crawler = fail_crawler
        _state.fail_runner = fail_crawler
        _state.fail_backup = fail_backup
        return _state

    @staticmethod
    def node_state(group: str = None, role: CrawlerStateRole = None) -> NodeState:
        _node_state = NodeState()
        if group is not None:
            _node_state.group = group
        if role is None:
            role = CrawlerStateRole.Initial
        _node_state.role = role
        return _node_state

    @staticmethod
    def task(task_result: TaskResult = None, task_content: dict = {}) -> Task:
        _task = Task()
        if task_result is None:
            task_result = TaskResult.Nothing
        _task.task_result = task_result
        _task.task_content = {}
        return _task

    @staticmethod
    def heartbeat(heart_rhythm_time: datetime = None, time_format: str = "%Y-%m-%d %H:%M:%S", update_time: str = "2s",
                  update_timeout: str = "4s", heart_rhythm_timeout: str = "3", healthy_state: HeartState = None,
                  task_state: TaskResult = None) -> Heartbeat:
        _heartbeat = Heartbeat()
        _heartbeat.heart_rhythm_time = datetime.now().strftime(time_format)
        _heartbeat.time_format = time_format
        _heartbeat.update_time = update_time
        _heartbeat.update_timeout = update_timeout
        _heartbeat.heart_rhythm_timeout = heart_rhythm_timeout
        if healthy_state is None:
            healthy_state = HeartState.Newborn
        _heartbeat.healthy_state = healthy_state
        if task_state is None:
            task_state = TaskResult.Nothing
        _heartbeat.task_state = task_state
        return _heartbeat


class Update(_BaseDataObjectUtils):
    """
    Update the data object with every different options.
    """

    @staticmethod
    def group_state(state: GroupState, total_crawler: int = None, total_runner: int = None, total_backup: int = None,
                    role: CrawlerStateRole = None, standby_id: str = None, append_current_crawler: List[str] = [],
                    append_current_runner: List[str] = [], append_current_backup: List[str] = [], append_fail_crawler: List[str] = [],
                    append_fail_runner: List[str] = [], append_fail_backup: List[str] = []) -> GroupState:

        Update._update_ele_if_not_none(data_obj=state, prop="total_crawler", new_val=total_crawler)
        Update._update_ele_if_not_none(data_obj=state, prop="total_runner", new_val=total_runner)
        Update._update_ele_if_not_none(data_obj=state, prop="total_backup", new_val=total_backup)

        Update._append_ele_if_not_none(data_obj=state, prop="current_crawler", new_val=append_current_crawler)
        Update._append_ele_if_not_none(data_obj=state, prop="current_runner", new_val=append_current_runner)
        Update._append_ele_if_not_none(data_obj=state, prop="current_backup", new_val=append_current_backup)

        Update._update_ele_if_not_none(data_obj=state, prop="standby_id", new_val=standby_id)

        Update._append_ele_if_not_none(data_obj=state, prop="fail_crawler", new_val=append_fail_crawler)
        Update._append_ele_if_not_none(data_obj=state, prop="fail_runner", new_val=append_fail_crawler)
        Update._append_ele_if_not_none(data_obj=state, prop="fail_backup", new_val=append_fail_backup)

        return state

    @staticmethod
    def node_state(node_state: NodeState, group: str = None, role: CrawlerStateRole = None) -> NodeState:
        Update._update_ele_if_not_none(data_obj=node_state, prop="group", new_val=group)
        Update._update_ele_if_not_none(data_obj=node_state, prop="role", new_val=role)
        return node_state

    @staticmethod
    def task(task: Task, task_result: TaskResult = None, task_content: dict = {}) -> Task:
        Update._update_ele_if_not_none(data_obj=task, prop="task_result", new_val=task_result)
        Update._update_ele_if_not_none(data_obj=task, prop="task_content", new_val=task_content)
        return task

    @staticmethod
    def heartbeat(heartbeat: Heartbeat, heart_rhythm_time: datetime = None, time_format: str = "%Y-%m-%d %H:%M:%S",
                  update_time: str = "2s", update_timeout: str = "4s", heart_rhythm_timeout: str = "3",
                  healthy_state: HeartState = None, task_state: TaskResult = None) -> Heartbeat:
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="heart_rhythm_time", new_val=heart_rhythm_time)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="time_format", new_val=time_format)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="update_time", new_val=update_time)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="update_timeout", new_val=update_timeout)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="heart_rhythm_timeout", new_val=heart_rhythm_timeout)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="healthy_state", new_val=healthy_state)
        Update._update_ele_if_not_none(data_obj=heartbeat, prop="task_state", new_val=task_state)
        return heartbeat

    @staticmethod
    def _update_ele_if_not_none(data_obj, prop: str, new_val: Union[int, str, dict, datetime, CrawlerStateRole, TaskResult, HeartState]):
        if new_val is not None:
            setattr(data_obj, prop, new_val)

    @staticmethod
    def _append_ele_if_not_none(data_obj, prop: str, new_val: List[str]):
        if new_val is not None:
            _prop_value = getattr(data_obj, prop)
            if _prop_value is None:
                _prop_value = []
            _prop_value += list(set(new_val))
            setattr(data_obj, prop, _prop_value)
