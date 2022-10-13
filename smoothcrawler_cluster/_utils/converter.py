from abc import ABCMeta, abstractmethod
from typing import Dict, Any
import json

from ..model.metadata import State, Task, Heartbeat


class BaseConverter(metaclass=ABCMeta):

    @abstractmethod
    def serialize(self, data: Any) -> str:
        pass

    @abstractmethod
    def deserialize(self, data: str) -> Any:
        pass


class JsonStrConverter(BaseConverter):

    def serialize(self, data: Any) -> str:
        # data maybe a str type value or a dict type value
        _data = json.dumps(data)
        return _data

    def deserialize(self, data: str) -> Any:
        _parsed_data: Dict[str, Any] = json.loads(data)
        return _parsed_data

    def state_to_str(self, state: State) -> str:
        return self.serialize(data=str(state))

    def task_to_str(self, task: Task) -> str:
        return self.serialize(data=str(task))

    def heartbeat_to_str(self, heartbeat: Heartbeat) -> str:
        return self.serialize(data=str(heartbeat))

    def str_to_state(self, data: str) -> State:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _state_with_value = self._convert_to_state(state=State(), data=_parsed_data)
        return _state_with_value

    def _convert_to_state(self, state: State, data: Any) -> State:
        data: Dict[str, Any] = data
        state.role = data.get("role")
        state.total_crawler = data.get("total_crawler")
        state.total_runner = data.get("total_runner")
        state.total_backup = data.get("total_backup")
        state.current_crawler = data.get("current_crawler")
        state.current_runner = data.get("current_runner")
        state.current_backup = data.get("current_backup")
        state.standby_id = data.get("standby_id")
        state.fail_crawler = data.get("fail_crawler")
        state.fail_runner = data.get("fail_runner")
        state.fail_backup = data.get("fail_backup")
        return state

    def str_to_task(self, data: str) -> Task:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _task_with_value = self._convert_to_task(task=Task(), data=_parsed_data)
        return _task_with_value

    def _convert_to_task(self, task: Task, data: Any) -> Task:
        data: Dict[str, Any] = data
        task.task_content = data.get("task_content")
        task.task_result = data.get("task_result")
        return task

    def str_to_heartbeat(self, data: str) -> Heartbeat:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _heartbeat_with_value = self._convert_to_heartbeat(heartbeat=Heartbeat(), data=_parsed_data)
        return _heartbeat_with_value

    def _convert_to_heartbeat(self, heartbeat: Heartbeat, data: Any) -> Heartbeat:
        data: Dict[str, Any] = data
        heartbeat.datetime = data.get("datetime")
        return heartbeat
