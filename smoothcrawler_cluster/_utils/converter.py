from abc import ABCMeta, abstractmethod
from typing import Dict, Any, TypeVar, Generic
import json

from ..model.metadata import _BaseMetaData, GroupState, NodeState, Task, Heartbeat


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)


class BaseConverter(metaclass=ABCMeta):

    @abstractmethod
    def _convert_to_str(self, data: Any) -> str:
        pass

    @abstractmethod
    def deserialize(self, data: str) -> Any:
        pass

    def group_state_to_str(self, state: GroupState) -> str:
        return self._convert_to_str(data=self._convert_to_readable_object(obj=state))

    def node_state_to_str(self, state: NodeState) -> str:
        return self._convert_to_str(data=self._convert_to_readable_object(obj=state))

    def task_to_str(self, task: Task) -> str:
        return self._convert_to_str(data=self._convert_to_readable_object(obj=task))

    def heartbeat_to_str(self, heartbeat: Heartbeat) -> str:
        return self._convert_to_str(data=self._convert_to_readable_object(obj=heartbeat))

    @abstractmethod
    def _convert_to_readable_object(self, obj: Generic[_BaseMetaDataType]) -> Any:
        pass

    def str_to_group_state(self, data: str) -> GroupState:
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _state_with_value = self._convert_to_group_state(state=GroupState(), data=_parsed_data)
        return _state_with_value

    @abstractmethod
    def _convert_to_group_state(self, state: GroupState, data: Any) -> GroupState:
        pass

    def str_to_node_state(self, data: str) -> NodeState:
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _state_with_value = self._convert_to_node_state(state=NodeState(), data=_parsed_data)
        return _state_with_value

    @abstractmethod
    def _convert_to_node_state(self, state: NodeState, data: Any) -> NodeState:
        pass

    def str_to_task(self, data: str) -> Task:
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _task_with_value = self._convert_to_task(task=Task(), data=_parsed_data)
        return _task_with_value

    @abstractmethod
    def _convert_to_task(self, task: Task, data: Any) -> Task:
        pass

    def str_to_heartbeat(self, data: str) -> Heartbeat:
        _parsed_data: Dict[str, Any] = self.deserialize(data=data)
        _heartbeat_with_value = self._convert_to_heartbeat(heartbeat=Heartbeat(), data=_parsed_data)
        return _heartbeat_with_value

    @abstractmethod
    def _convert_to_heartbeat(self, heartbeat: Heartbeat, data: Any) -> Heartbeat:
        pass


class JsonStrConverter(BaseConverter):

    def _convert_to_str(self, data: Any) -> str:
        # data maybe a str type value or a dict type value
        _data = json.dumps(data)
        return _data

    def deserialize(self, data: str) -> Any:
        _parsed_data: Dict[str, Any] = json.loads(data)
        return _parsed_data

    def _convert_to_readable_object(self, obj: Generic[_BaseMetaDataType]) -> Any:
        # TODO:
        #  1. Rename the function name to be more clear
        return obj.to_readable_object()

    def _convert_to_group_state(self, state: GroupState, data: Any) -> GroupState:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        data: Dict[str, Any] = data
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

    def _convert_to_node_state(self, state: NodeState, data: Any) -> NodeState:
        data: Dict[str, Any] = data
        state.group = data.get("group")
        state.role = data.get("role")
        return state

    def _convert_to_task(self, task: Task, data: Any) -> Task:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        data: Dict[str, Any] = data
        task.task_content = data.get("task_content")
        task.task_result = data.get("task_result")
        return task

    def _convert_to_heartbeat(self, heartbeat: Heartbeat, data: Any) -> Heartbeat:
        # TODO: Maybe it could develop a package like mapstruct in kotlin.
        data: Dict[str, Any] = data
        heartbeat.heart_rhythm_time = data.get("heart_rhythm_time")
        heartbeat.time_format = data.get("time_format")
        heartbeat.update_time = data.get("update_time")
        heartbeat.update_timeout = data.get("update_timeout")
        heartbeat.heart_rhythm_timeout = data.get("heart_rhythm_timeout")
        heartbeat.healthy_state = data.get("healthy_state")
        heartbeat.task_state = data.get("task_state")
        return heartbeat
