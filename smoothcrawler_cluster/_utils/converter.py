"""*Util functions of serializing and deserializing of data from Zookeeper*

No matter operating data as getting from or setting to Zookeeper, it must be byte or string type value. Therefore, the
serializing or deserializing features would be deeply necessary in this package. Here module for all the features about
serializing and deserializing.
"""

from abc import ABCMeta, abstractmethod
from typing import Dict, Any, Type, TypeVar, Generic
import json

from ..model.metadata import (
    # Base class
    _BaseMetaData,
    # Meta-Data objects
    GroupState, NodeState, Task, Heartbeat,
    # Objects in meta-data
    RunningContent, RunningResult, ResultDetail
)


_BaseMetaDataType = TypeVar("_BaseMetaDataType", bound=_BaseMetaData)


class BaseConverter(metaclass=ABCMeta):
    """*The base class of serializing or deserializing features*

    In addiction to serializing or deserializing features, it also rules some functions of operating (serialize or
    deserialize) with some needed meta-data objects.
    """

    def serialize_meta_data(self, obj: Generic[_BaseMetaDataType]) -> str:
        """Serialize data as string type value.

        Args:
            obj (Generic[_BaseMetaDataType]): Target object to serialize.

        Returns:
            str: The serializing result. It must be a string type value.

        """
        return self._convert_to_str(data=self._convert_to_readable_object(obj))

    def deserialize_meta_data(self, data: str, as_obj: Type[_BaseMetaDataType]) -> Generic[_BaseMetaDataType]:
        """Deserialize the string type value to be one specific type object in Python.

        Args:
            data (str): The string type value to deserialize.
            as_obj (Type[_BaseMetaDataType]): The target object it deserializes value to be.

        Returns:
            Generic[_BaseMetaDataType]: The meta-data instance from deserializing. It would be the instance of object
                type from argument *as_obj*, i.e., it would return **GroupState** instance if value of argument *as_obj*
                is it.

            .. code-block:: python

                >>> metadata = <BaseConverter type instance>.deserialize_meta_data(data=data, as_obj=GroupState)
                >>> type(metadata)
                <class 'GroupState'>

        Raises:
            TypeError: If the value type of argument *as_obj* DOES NOT one of these 4 types meta-data:
                :ref:`GroupState <MetaData_GroupState>`, :ref:`NodeState <MetaData_NodeState>`,
                :ref:`Task <MetaData_Task>` and :ref:`Heartbeat <MetaData_Heartbeat>`.

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
        """Serialize the data as string type value from an Any type object.

        Args:
            data (Any): Target data to serialize.

        Returns:
            str: The serializing result. It must be a string type value.

        """
        pass

    @abstractmethod
    def _convert_from_str(self, data: str) -> Any:
        """Deserialize the data to Any type object from string type value.

        Args:
            data (str): A string type value to deserialize.

        Returns:
            Any: The final object which be deserialized. It could be customized object.

        """
        pass

    @abstractmethod
    def _convert_to_readable_object(self, obj: Generic[_BaseMetaDataType]) -> Any:
        """It would converse the object as dict type value (converse to JSON format data before serializing) via the
        common function *to_readable_object* which be defined in :ref:`base class of meta-data <MetaData_BaseClass>`.

        Args:
            obj (Generic[_BaseMetaDataType]): The _BaseMetaData type object.

        Returns:
            Any: Any type which could be serialized as string type value. In generally, it could be dict type which
                mostly like JSON format value.

        """
        pass

    @abstractmethod
    def _convert_to_group_state(self, state: GroupState, data: Any) -> GroupState:
        """Converse the data as **GroupState** type instance.

        Args:
            state (GroupState): The instance of GroupState.
            data (Any): The target data to deserialize.

        Returns:
            GroupState: The meta-data **GroupState** instance keeps the values be deserialized from Zookeeper.

        """
        pass

    @abstractmethod
    def _convert_to_node_state(self, state: NodeState, data: Any) -> NodeState:
        """Converse the data as **NodeState** type instance.

        Args:
            state (NodeState): The instance of NodeState.
            data (Any): The target data to deserialize.

        Returns:
            NodeState: The meta-data **NodeState** instance keeps the values be deserialized from Zookeeper.

        """
        pass

    @abstractmethod
    def _convert_to_task(self, task: Task, data: Any) -> Task:
        """Converse the data as **Task** type instance.

        Args:
            task (Task): The instance of Task.
            data (Any): The target data to deserialize.

        Returns:
            Task: The meta-data **Task** instance keeps the values be deserialized from Zookeeper.

        """
        pass

    @abstractmethod
    def _convert_to_heartbeat(self, heartbeat: Heartbeat, data: Any) -> Heartbeat:
        """Converse the data as **Heartbeat** type instance.

        Args:
            heartbeat (Heartbeat): The instance of Heartbeat.
            data (Any): The target data to deserialize.

        Returns:
            Heartbeat: The meta-data **Heartbeat** instance keeps the values be deserialized from Zookeeper.

        """
        pass


class JsonStrConverter(BaseConverter):
    """*Operating with JSON format data*

    It would operate with JSON format data to do serialize or deserialize, in the other words, it would try to serialize
    `json object`_ to be string type value, or deserialize JSON format string data as *json object* in Python.

    .. _json object: https://docs.python.org/3/library/json.html
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
    """*Detail data in meta-data*

    For some meta-data, e.g., **Task**, it has a little bit complex meta-data structure. For example, its option
    *running_result* is also a dict type value, options *running_content* and *result_detail* even a list which
    element type is dict in list. For more clear and better to develop and maintain, it has `namedtuple`_ object
    to do data processing. And this util class for conversing dict type value as namedtuple object.

    .. _namedtuple: https://docs.python.org/3/library/collections.html#collections.namedtuple
    """

    @staticmethod
    def convert_to_running_content(data: dict) -> RunningContent:
        """Converse data to namedtuple **RunningContent**.

        Args:
            data (dict): The data which be keep by option *running_content* in **Task** meta-data.

        Returns:
            RunningContent: A namedtuple type object.

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
        """Converse data to namedtuple **RunningResult**.

        Args:
            data (dict): The data which be keep by option *running_result* in **Task** meta-data.

        Returns:
            RunningResult: A namedtuple type object.

        """
        return RunningResult(success_count=data["success_count"], fail_count=data["fail_count"])

    @staticmethod
    def convert_to_result_detail(data: dict) -> ResultDetail:
        """Converse data to namedtuple **RunningContent**.

        Args:
            data (dict): The data which be keep by option *result_detail* in **Task** meta-data.

        Returns:
            RunningContent: A namedtuple type object.

        """
        return ResultDetail(
            task_id=data["task_id"],
            state=data["state"],
            status_code=data["status_code"],
            response=data["response"],
            error_msg=data["error_msg"]
        )
