from smoothcrawler_cluster.model.metadata import GroupState, NodeState, Task, Heartbeat
from typing import List, Union
from abc import ABCMeta, abstractmethod
import json


class GroupStateData(metaclass=ABCMeta):

    def __init__(self, data: Union[str, bytes, GroupState]):
        self._data = data

    @property
    @abstractmethod
    def total_crawler(self) -> int:
        pass

    @property
    @abstractmethod
    def total_runner(self) -> int:
        pass

    @property
    @abstractmethod
    def total_backup(self) -> int:
        pass

    @property
    @abstractmethod
    def current_crawler(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def current_runner(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def current_backup(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def standby_id(self) -> str:
        pass

    @property
    @abstractmethod
    def fail_crawler(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def fail_runner(self) -> List[str]:
        pass

    @property
    @abstractmethod
    def fail_backup(self) -> List[str]:
        pass


class GroupStateByObject(GroupStateData):

    @property
    def total_crawler(self) -> int:
        return self._data.total_crawler

    @property
    def total_runner(self) -> int:
        return self._data.total_runner

    @property
    def total_backup(self) -> int:
        return self._data.total_backup

    @property
    def current_crawler(self) -> List[str]:
        return self._data.current_crawler

    @property
    def current_runner(self) -> List[str]:
        return self._data.current_runner

    @property
    def current_backup(self) -> List[str]:
        return self._data.current_backup

    @property
    def standby_id(self) -> str:
        return self._data.standby_id

    @property
    def fail_crawler(self) -> List[str]:
        return self._data.fail_crawler

    @property
    def fail_runner(self) -> List[str]:
        return self._data.fail_runner

    @property
    def fail_backup(self) -> List[str]:
        return self._data.fail_backup


class _GetDataFromJsonData(metaclass=ABCMeta):

    def __init__(self, data: Union[str, bytes]):
        if type(data) is bytes:
            _json_format_data = data.decode("utf-8")
        elif type(data) is str:
            _json_format_data = data
        else:
            raise TypeError(f"Doesn't support type {type(data)} processing.")

        self._data = json.loads(_json_format_data)


class GroupStateByJsonData(_GetDataFromJsonData, GroupStateData):

    def __init__(self, data: Union[str, bytes]):
        super(GroupStateByJsonData, self).__init__(data=data)

    @property
    def total_crawler(self) -> int:
        return self._data["total_crawler"]

    @property
    def total_runner(self) -> int:
        return self._data["total_runner"]

    @property
    def total_backup(self) -> int:
        return self._data["total_backup"]

    @property
    def current_crawler(self) -> List[str]:
        return self._data["current_crawler"]

    @property
    def current_runner(self) -> List[str]:
        return self._data["current_runner"]

    @property
    def current_backup(self) -> List[str]:
        return self._data["current_backup"]

    @property
    def standby_id(self) -> str:
        return self._data["standby_id"]

    @property
    def fail_crawler(self) -> List[str]:
        return self._data["fail_crawler"]

    @property
    def fail_runner(self) -> List[str]:
        return self._data["fail_runner"]

    @property
    def fail_backup(self) -> List[str]:
        return self._data["fail_backup"]


class NodeStateData(metaclass=ABCMeta):

    def __init__(self, data: Union[str, bytes, NodeState]):
        self._data = data

    @property
    @abstractmethod
    def role(self) -> str:
        pass

    @property
    @abstractmethod
    def group(self) -> str:
        pass


class NodeStateByObject(NodeStateData):

    @property
    def role(self) -> str:
        return self._data.role

    @property
    def group(self) -> str:
        return self._data.group


class NodeStateByJsonData(_GetDataFromJsonData, NodeStateData):

    @property
    def role(self) -> str:
        return self._data["role"]

    @property
    def group(self) -> str:
        return self._data["group"]


class TaskData(metaclass=ABCMeta):

    def __init__(self, data: Union[str, bytes, Task]):
        self._data = data

    @property
    @abstractmethod
    def running_content(self) -> List:
        pass

    @property
    @abstractmethod
    def cookies(self) -> dict:
        pass

    @property
    @abstractmethod
    def authorization(self) -> dict:
        pass

    @property
    @abstractmethod
    def in_progressing_id(self) -> str:
        pass

    @property
    @abstractmethod
    def running_result(self) -> dict:
        pass

    @property
    @abstractmethod
    def running_status(self) -> str:
        pass

    @property
    @abstractmethod
    def result_detail(self) -> List[dict]:
        pass


class TaskDataFromObject(TaskData):

    @property
    def running_content(self) -> List:
        return self._data.running_content

    @property
    def cookies(self) -> dict:
        return self._data.cookie

    @property
    def authorization(self) -> dict:
        return self._data.authorization

    @property
    def in_progressing_id(self) -> str:
        return self._data.in_progressing_id

    @property
    def running_result(self) -> dict:
        return self._data.running_result

    @property
    def running_status(self) -> str:
        return self._data.running_status

    @property
    def result_detail(self) -> List[dict]:
        return self._data.result_detail


class TaskDataFromJsonData(_GetDataFromJsonData, TaskData):

    @property
    def running_content(self) -> List:
        return self._data["running_content"]

    @property
    def cookies(self) -> dict:
        return self._data["cookie"]

    @property
    def authorization(self) -> dict:
        return self._data["authorization"]

    @property
    def in_progressing_id(self) -> str:
        return self._data["in_progressing_id"]

    @property
    def running_result(self) -> dict:
        return self._data["running_result"]

    @property
    def running_status(self) -> str:
        return self._data["running_status"]

    @property
    def result_detail(self) -> List[dict]:
        return self._data["result_detail"]


class HeartbeatData(metaclass=ABCMeta):

    def __init__(self, data: Union[str, bytes, Heartbeat]):
        self._data = data

    @property
    @abstractmethod
    def heart_rhythm_time(self) -> str:
        pass

    @property
    @abstractmethod
    def time_format(self) -> str:
        pass

    @property
    @abstractmethod
    def update_time(self) -> str:
        pass

    @property
    @abstractmethod
    def update_timeout(self) -> str:
        pass

    @property
    @abstractmethod
    def heart_rhythm_timeout(self) -> str:
        pass

    @property
    @abstractmethod
    def healthy_state(self) -> str:
        pass

    @property
    @abstractmethod
    def task_state(self) -> str:
        pass


class HeartbeatFromObject(HeartbeatData):

    @property
    def heart_rhythm_time(self) -> str:
        return self._data.heart_rhythm_time

    @property
    def time_format(self) -> str:
        return self._data.time_format

    @property
    def update_time(self) -> str:
        return self._data.update_time

    @property
    def update_timeout(self) -> str:
        return self._data.update_timeout

    @property
    def heart_rhythm_timeout(self) -> str:
        return self._data.heart_rhythm_timeout

    @property
    def healthy_state(self) -> str:
        return self._data.healthy_state

    @property
    def task_state(self) -> str:
        return self._data.task_state


class HeartbeatFromJsonData(_GetDataFromJsonData, HeartbeatData):

    @property
    def heart_rhythm_time(self) -> str:
        return self._data["heart_rhythm_time"]

    @property
    def time_format(self) -> str:
        return self._data["time_format"]

    @property
    def update_time(self) -> str:
        return self._data["update_time"]

    @property
    def update_timeout(self) -> str:
        return self._data["update_timeout"]

    @property
    def heart_rhythm_timeout(self) -> str:
        return self._data["heart_rhythm_timeout"]

    @property
    def healthy_state(self) -> str:
        return self._data["healthy_state"]

    @property
    def task_state(self) -> str:
        return self._data["task_state"]
