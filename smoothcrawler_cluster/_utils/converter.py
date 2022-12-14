"""Module document
"""

from typing import Dict, Any, Type, TypeVar, Generic
from abc import ABCMeta, abstractmethod
import json

from ..model.metadata import _BaseMetaData, GroupState, NodeState, Task, RunningContent, RunningResult, ResultDetail, Heartbeat


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)


class BaseConverter(metaclass=ABCMeta):
    """Class document
    """

    def serialize_meta_data(self, obj: Generic[_BaseMetaDataType]) -> str:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        return self._convert_to_str(data=self._convert_to_readable_object(obj))

    def deserialize_meta_data(self, data: str, as_obj: Type[_BaseMetaDataType]) -> Generic[_BaseMetaDataType]:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        parsed_data: Dict[str, Any] = self._convert_from_str(data=data)
        if issubclass(as_obj, GroupState):
            meta_data_obj = self._convert_to_group_state(state=as_obj(), data=parsed_data)
        elif issubclass(as_obj, NodeState):
            meta_data_obj = self._convert_to_node_state(state=as_obj(), data=parsed_data)
        elif issubclass(as_obj, Task):
            meta_data_obj = self._convert_to_task(task=as_obj(), data=parsed_data)
        elif issubclass(as_obj, Heartbeat):
            meta_data_obj = self._convert_to_heartbeat(heartbeat=as_obj(), data=parsed_data)
        else:
            raise TypeError(f"It doesn't support deserialize data as type '{as_obj}' renctly.")
        return meta_data_obj

    @abstractmethod
    def _convert_to_str(self, data: Any) -> str:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        pass

    @abstractmethod
    def _convert_from_str(self, data: str) -> Any:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        pass

    @abstractmethod
    def _convert_to_readable_object(self, obj: Generic[_BaseMetaDataType]) -> Any:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        pass

    @abstractmethod
    def _convert_to_group_state(self, state: GroupState, data: Any) -> GroupState:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        pass

    @abstractmethod
    def _convert_to_node_state(self, state: NodeState, data: Any) -> NodeState:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        pass

    @abstractmethod
    def _convert_to_task(self, task: Task, data: Any) -> Task:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        pass

    @abstractmethod
    def _convert_to_heartbeat(self, heartbeat: Heartbeat, data: Any) -> Heartbeat:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        pass


class JsonStrConverter(BaseConverter):
    """Class document
    """

    def _convert_to_str(self, data: Any) -> str:
        # data maybe a str type value or a dict type value
        data = json.dumps(data)
        return data

    def _convert_from_str(self, data: str) -> Any:
        parsed_data: Dict[str, Any] = json.loads(data)
        return parsed_data

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


class TaskContentDataUtils:
    """Class document
    """

    @staticmethod
    def convert_to_running_content(data: dict) -> RunningContent:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        return RunningContent(
            task_id=data["task_id"],
            url=data["url"],
            method=data["method"],
            parameters=data["parameters"],
            header=data["header"],
            body=data["body"]
        )

    @staticmethod
    def convert_to_running_result(data: dict) -> RunningResult:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        return RunningResult(success_count=data["success_count"], fail_count=data["fail_count"])

    @staticmethod
    def convert_to_result_detail(data: dict) -> ResultDetail:
        """Function Doc

        Args:
            obj:

        Returns:

        """
        return ResultDetail(
            task_id=data["task_id"],
            state=data["state"],
            status_code=data["status_code"],
            response=data["response"],
            error_msg=data["error_msg"]
        )
