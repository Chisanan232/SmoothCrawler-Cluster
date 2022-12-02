from smoothcrawler_cluster.model import GroupState, NodeState, Task, Heartbeat
from smoothcrawler_cluster.crawler import ZookeeperCrawler
import json

from .._values import (
    # GroupState
    _Runner_Crawler_Value, _Backup_Crawler_Value,
    # common functions
    setup_group_state, setup_node_state, setup_task, setup_heartbeat
)


class _TestValue:

    __Test_Value_Instance = None

    __Group_State_ZK_Path: str = ""
    __Node_State_ZK_Path: str = ""
    __Task_ZK_Path: str = ""
    __Heartbeat_ZK_Path: str = ""

    __Testing_Group_State_Data_Str: str = ""
    __Testing_Node_State_Data_Str: str = ""
    __Testing_Task_Data_Str: str = ""
    __Testing_Heartbeat_Data_Str: str = ""

    __Testing_Group_State: GroupState = None
    __Testing_Node_State: NodeState = None
    __Testing_Task: Task = None
    __Testing_Heartbeat: Heartbeat = None

    def __new__(cls, *args, **kwargs):
        if cls.__Test_Value_Instance is None:
            cls.__Test_Value_Instance = super(_TestValue, cls).__new__(cls, *args, **kwargs)
        return cls.__Test_Value_Instance

    def __init__(self):
        self._zk_client_inst = ZookeeperCrawler(runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, initial=False)

    @property
    def group_state_zookeeper_path(self) -> str:
        if self.__Group_State_ZK_Path == "":
            self.__Group_State_ZK_Path = self._zk_client_inst.group_state_zookeeper_path
        return self.__Group_State_ZK_Path

    @property
    def node_state_zookeeper_path(self) -> str:
        if self.__Node_State_ZK_Path == "":
            self.__Node_State_ZK_Path = self._zk_client_inst.node_state_zookeeper_path
        return self.__Node_State_ZK_Path

    @property
    def task_zookeeper_path(self) -> str:
        if self.__Task_ZK_Path == "":
            self.__Task_ZK_Path = self._zk_client_inst.task_zookeeper_path
        return self.__Task_ZK_Path

    @property
    def heartbeat_zookeeper_path(self) -> str:
        if self.__Heartbeat_ZK_Path == "":
            self.__Heartbeat_ZK_Path = self._zk_client_inst.heartbeat_zookeeper_path
        return self.__Heartbeat_ZK_Path

    @property
    def group_state(self) -> GroupState:
        if self.__Testing_Group_State is None:
            self.__Testing_Group_State = setup_group_state(reset=True)
        return self.__Testing_Group_State

    @property
    def node_state(self) -> NodeState:
        if self.__Testing_Node_State is None:
            self.__Testing_Node_State = setup_node_state()
        return self.__Testing_Node_State

    @property
    def task(self) -> Task:
        if self.__Testing_Task is None:
            self.__Testing_Task = setup_task(reset=True)
        return self.__Testing_Task

    @property
    def heartbeat(self) -> Heartbeat:
        if self.__Testing_Heartbeat is None:
            self.__Testing_Heartbeat = setup_heartbeat()
        return self.__Testing_Heartbeat

    @property
    def group_state_data_str(self) -> str:
        if self.__Testing_Group_State_Data_Str == "":
            self.__Testing_Group_State_Data_Str = json.dumps(self.group_state.to_readable_object())
        return self.__Testing_Group_State_Data_Str

    @property
    def node_state_data_str(self) -> str:
        if self.__Testing_Node_State_Data_Str == "":
            self.__Testing_Node_State_Data_Str = json.dumps(self.node_state.to_readable_object())
        return self.__Testing_Node_State_Data_Str

    @property
    def task_data_str(self) -> str:
        if self.__Testing_Task_Data_Str == "":
            self.__Testing_Task_Data_Str = json.dumps(self.task.to_readable_object())
        return self.__Testing_Task_Data_Str

    @property
    def heartbeat_data_str(self) -> str:
        if self.__Testing_Heartbeat_Data_Str == "":
            self.__Testing_Heartbeat_Data_Str = json.dumps(self.heartbeat.to_readable_object())
        return self.__Testing_Heartbeat_Data_Str

