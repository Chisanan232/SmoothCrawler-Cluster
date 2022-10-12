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

    def str_to_state(self, data: str) -> State:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _state = State()

        _state.role = _parsed_data.get("role")
        _state.total_crawler = _parsed_data.get("total_crawler")
        _state.total_runner = _parsed_data.get("total_runner")
        _state.total_backup = _parsed_data.get("total_backup")
        _state.current_crawler = _parsed_data.get("current_crawler")
        _state.current_runner = _parsed_data.get("current_runner")
        _state.current_backup = _parsed_data.get("current_backup")
        _state.standby_id = _parsed_data.get("standby_id")
        _state.fail_crawler = _parsed_data.get("fail_crawler")
        _state.fail_runner = _parsed_data.get("fail_runner")
        _state.fail_backup = _parsed_data.get("fail_backup")

        return _state

    def str_to_task(self, data: str) -> Task:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _task = Task()

        _task.task_content = _parsed_data.get("task_content")
        _task.task_result = _parsed_data.get("task_result")

        return _task

    def str_to_heartbeat(self, data: str) -> Heartbeat:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _heartbeat = Heartbeat()

        _heartbeat.datetime = _parsed_data.get("datetime")

        return _heartbeat
