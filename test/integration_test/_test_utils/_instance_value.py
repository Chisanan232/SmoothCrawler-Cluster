import json
from typing import List

from smoothcrawler_cluster._utils.zookeeper import ZookeeperPath
from smoothcrawler_cluster.model import GroupState, Heartbeat, NodeState, Task

from ..._values import (
    _Crawler_Group_Name_Value,
    _Crawler_Name_Value,
    setup_group_state,
    setup_heartbeat,
    setup_node_state,
    setup_task,
)


class _TestValue:

    _test_value_instance = None

    _group_state_zk_path: str = ""
    _node_state_zk_path: str = ""
    _task_zk_path: str = ""
    _heartbeat_zk_path: str = ""

    _testing_group_state_data_str: str = ""
    _testing_node_state_data_str: str = ""
    _testing_task_data_str: str = ""
    _testing_heartbeat_data_str: str = ""

    _testing_group_state: GroupState = None
    _testing_node_state: NodeState = None
    _testing_task: Task = None
    _testing_heartbeat: Heartbeat = None

    def __new__(cls, *args, **kwargs):
        if cls._test_value_instance is None:
            cls._test_value_instance = super(_TestValue, cls).__new__(cls, *args, **kwargs)
        return cls._test_value_instance

    def __init__(self):
        self._zk_path = ZookeeperPath(name=_Crawler_Name_Value, group=_Crawler_Group_Name_Value)

    @property
    def name(self):
        return _Crawler_Name_Value

    @property
    def group(self):
        return _Crawler_Group_Name_Value

    @property
    def group_state_zookeeper_path(self) -> str:
        if self._group_state_zk_path == "":
            self._group_state_zk_path = self._zk_path.group_state
        return self._group_state_zk_path

    @property
    def node_state_zookeeper_path(self) -> str:
        if self._node_state_zk_path == "":
            self._node_state_zk_path = self._zk_path.node_state
        return self._node_state_zk_path

    @property
    def task_zookeeper_path(self) -> str:
        if self._task_zk_path == "":
            self._task_zk_path = self._zk_path.task
        return self._task_zk_path

    @property
    def heartbeat_zookeeper_path(self) -> str:
        if self._heartbeat_zk_path == "":
            self._heartbeat_zk_path = self._zk_path.heartbeat
        return self._heartbeat_zk_path

    @property
    def group_state(self) -> GroupState:
        if self._testing_group_state is None:
            self._testing_group_state = setup_group_state(reset=True)
        return self._testing_group_state

    @property
    def node_state(self) -> NodeState:
        if self._testing_node_state is None:
            self._testing_node_state = setup_node_state()
        return self._testing_node_state

    @property
    def task(self) -> Task:
        if self._testing_task is None:
            self._testing_task = setup_task(reset=True)
        return self._testing_task

    @property
    def heartbeat(self) -> Heartbeat:
        if self._testing_heartbeat is None:
            self._testing_heartbeat = setup_heartbeat()
        return self._testing_heartbeat

    @property
    def group_state_data_str(self) -> str:
        if self._testing_group_state_data_str == "":
            self._testing_group_state_data_str = json.dumps(self.group_state.to_readable_object())
        return self._testing_group_state_data_str

    @property
    def node_state_data_str(self) -> str:
        if self._testing_node_state_data_str == "":
            self._testing_node_state_data_str = json.dumps(self.node_state.to_readable_object())
        return self._testing_node_state_data_str

    @property
    def task_data_str(self) -> str:
        if self._testing_task_data_str == "":
            self._testing_task_data_str = json.dumps(self.task.to_readable_object())
        return self._testing_task_data_str

    @property
    def heartbeat_data_str(self) -> str:
        if self._testing_heartbeat_data_str == "":
            self._testing_heartbeat_data_str = json.dumps(self.heartbeat.to_readable_object())
        return self._testing_heartbeat_data_str


class _ZKNodePathUtils:

    _testing_value: _TestValue = _TestValue()

    @classmethod
    def all(cls, size: int, start_index: int = 1) -> List[str]:
        all_paths = [cls._testing_value.group_state_zookeeper_path]
        all_paths.extend(cls.all_node_state(size, start_index))
        all_paths.extend(cls.all_task(size, start_index))
        all_paths.extend(cls.all_heartbeat(size, start_index))
        return all_paths

    @classmethod
    def all_node_state(cls, size: int, start_index: int = 1) -> List[str]:
        return cls._opt_paths_list(size, cls._testing_value.node_state_zookeeper_path, start_index)

    @classmethod
    def all_task(cls, size: int, start_index: int = 1) -> List[str]:
        return cls._opt_paths_list(size, cls._testing_value.task_zookeeper_path, start_index)

    @classmethod
    def all_heartbeat(cls, size: int, start_index: int = 1) -> List[str]:
        return cls._opt_paths_list(size, cls._testing_value.heartbeat_zookeeper_path, start_index)

    @classmethod
    def _opt_paths_list(cls, size: int, metadata_path: str, start_index: int = 1) -> List[str]:
        target_list = []
        for i in range(start_index, size + start_index):
            target_list.append(metadata_path.replace("1", str(i)))
        return target_list
