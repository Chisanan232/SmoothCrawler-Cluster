from typing import Dict, Any, Type, TypeVar, Generic
from abc import ABCMeta, abstractmethod
import json

from ..model.metadata import _BaseMetaData, GroupState, NodeState, Task, Heartbeat


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)


class BaseConverter(metaclass=ABCMeta):

    def serialize_meta_data(self, obj: Generic[_BaseMetaDataType]) -> str:
        return self._convert_to_str(data=self._convert_to_readable_object(obj))

    def deserialize_meta_data(self, data: str, as_obj: Type[_BaseMetaDataType]) -> Generic[_BaseMetaDataType]:
        _parsed_data: Dict[str, Any] = self._convert_from_str(data=data)
        if issubclass(as_obj, GroupState):
            _meta_data_obj = self._convert_to_group_state(state=as_obj(), data=_parsed_data)
        elif issubclass(as_obj, NodeState):
            _meta_data_obj = self._convert_to_node_state(state=as_obj(), data=_parsed_data)
        elif issubclass(as_obj, Task):
            _meta_data_obj = self._convert_to_task(task=as_obj(), data=_parsed_data)
        elif issubclass(as_obj, Heartbeat):
            _meta_data_obj = self._convert_to_heartbeat(heartbeat=as_obj(), data=_parsed_data)
        else:
            raise TypeError(f"It doesn't support deserialize data as type '{as_obj}' renctly.")
        return _meta_data_obj

    @abstractmethod
    def _convert_to_str(self, data: Any) -> str:
        pass

    @abstractmethod
    def _convert_from_str(self, data: str) -> Any:
        pass

    @abstractmethod
    def _convert_to_readable_object(self, obj: Generic[_BaseMetaDataType]) -> Any:
        pass

    @abstractmethod
    def _convert_to_group_state(self, state: GroupState, data: Any) -> GroupState:
        pass

    @abstractmethod
    def _convert_to_node_state(self, state: NodeState, data: Any) -> NodeState:
        pass

    @abstractmethod
    def _convert_to_task(self, task: Task, data: Any) -> Task:
        pass

    @abstractmethod
    def _convert_to_heartbeat(self, heartbeat: Heartbeat, data: Any) -> Heartbeat:
        pass


class JsonStrConverter(BaseConverter):

    def _convert_to_str(self, data: Any) -> str:
        # data maybe a str type value or a dict type value
        _data = json.dumps(data)
        return _data

    def _convert_from_str(self, data: str) -> Any:
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
        task.running_content = data.get("running_content")
        task.cookie = data.get("cookie")
        task.authorization = data.get("authorization")
        task.in_progressing_id = data.get("in_progressing_id")
        task.running_result = data.get("running_result")
        task.running_status = data.get("running_status")
        task.result_detail = data.get("result_detail")
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
