from datetime import datetime
from typing import List, Union
from abc import ABCMeta, abstractmethod

from .metadata import GroupState, Task, Heartbeat
from .metadata_enum import CrawlerStateRole, TaskResult, HeartState


class _BaseDataObjectUtils(metaclass=ABCMeta):

    @staticmethod
    @abstractmethod
    def state(*args, **kwargs) -> GroupState:
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
    def state() -> GroupState:
        _state = GroupState()
        _state.total_crawler = 0
        _state.total_runner = 0
        _state.total_backup = 0
        _state.role = CrawlerStateRole.Initial
        _state.current_crawler = []
        _state.current_runner = []
        _state.current_backup = []
        _state.standby_id = "0"
        _state.fail_crawler = []
        _state.fail_runner = []
        _state.fail_backup = []
        return _state

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
    def state(crawler_name: str, total_crawler: int, total_runner: int, total_backup: int, role: CrawlerStateRole = None,
              standby_id: str = "0", current_crawler: List[str] = [], current_runner: List[str] = [], current_backup: List[str] = [],
              fail_crawler: List[str] = [], fail_runner: List[str] = [], fail_backup: List[str] = []) -> GroupState:
        _state = GroupState()
        _state.total_crawler = total_crawler
        _state.total_runner = total_runner
        _state.total_backup = total_backup
        if role is None:
            role = CrawlerStateRole.Initial
        _state.role = role
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
    def state(state: GroupState, total_crawler: int = None, total_runner: int = None, total_backup: int = None,
              role: CrawlerStateRole = None, standby_id: str = None, append_current_crawler: List[str] = [],
              append_current_runner: List[str] = [], append_current_backup: List[str] = [], append_fail_crawler: List[str] = [],
              append_fail_runner: List[str] = [], append_fail_backup: List[str] = []) -> GroupState:

        def _update_ele_if_not_none(prop: str, new_val: Union[int, str, CrawlerStateRole]):
            if new_val is not None:
                setattr(state, prop, new_val)

        def _append_ele_if_not_none(prop: str, new_val: List[str]):
            if new_val is not None:
                _prop_value = getattr(state, prop)
                if _prop_value is None:
                    _prop_value = []
                _prop_value += list(set(new_val))
                setattr(state, prop, _prop_value)

        _update_ele_if_not_none(prop="total_crawler", new_val=total_crawler)
        _update_ele_if_not_none(prop="total_runner", new_val=total_runner)
        _update_ele_if_not_none(prop="total_backup", new_val=total_backup)

        _update_ele_if_not_none(prop="role", new_val=role)

        _append_ele_if_not_none(prop="current_crawler", new_val=append_current_crawler)
        _append_ele_if_not_none(prop="current_runner", new_val=append_current_runner)
        _append_ele_if_not_none(prop="current_backup", new_val=append_current_backup)

        _update_ele_if_not_none(prop="standby_id", new_val=standby_id)

        _append_ele_if_not_none(prop="fail_crawler", new_val=append_fail_crawler)
        _append_ele_if_not_none(prop="fail_runner", new_val=append_fail_crawler)
        _append_ele_if_not_none(prop="fail_backup", new_val=append_fail_backup)

        return state

    @staticmethod
    def task(task: Task, task_result: TaskResult = None, task_content: dict = {}) -> Task:
        if task_result is not None:
            task.task_result = task_result
        if task_content is not None:
            task.task_content = {}
        return task

    @staticmethod
    def heartbeat(heartbeat: Heartbeat, heart_rhythm_time: datetime = None, time_format: str = "%Y-%m-%d %H:%M:%S",
                  update_time: str = "2s", update_timeout: str = "4s", heart_rhythm_timeout: str = "3",
                  healthy_state: HeartState = None, task_state: TaskResult = None) -> Heartbeat:
        if heart_rhythm_time is not None:
            heartbeat.heart_rhythm_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if time_format is not None:
            heartbeat.time_format = time_format
        if update_time is not None:
            heartbeat.update_time = update_time
        if update_timeout is not None:
            heartbeat.update_timeout = update_timeout
        if heart_rhythm_timeout is not None:
            heartbeat.heart_rhythm_timeout = heart_rhythm_timeout
        if healthy_state is not None:
            heartbeat.healthy_state = healthy_state
        if task_state is not None:
            heartbeat.task_state = task_state
        return heartbeat
