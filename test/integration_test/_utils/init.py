from kazoo.client import KazooClient
from smoothcrawler_cluster.model import GroupState, NodeState, Task, Heartbeat
from smoothcrawler_cluster._utils import MetaDataUtil, ZookeeperClient, JsonStrConverter
import pytest

from ..._config import Zookeeper_Hosts
from ..._values import (
    # GroupState
    _Runner_Crawler_Value, _Backup_Crawler_Value, _Total_Crawler_Value, _Crawler_Role_Value, _State_Standby_ID_Value,
    # NodeState
    _Crawler_Group_Name_Value,
    # Task
    _Task_Running_Content_Value, _Task_Running_State,
    # Heartbeat
    _Time_Value, _Time_Format_Value,
)
from .._test_utils._instance_value import _TestValue
from .._test_utils._zk_testsuite import ZK, ZKNode, ZKTestSpec


_Not_None_Assertion_Error: str = "It should not be None object."
_Testing_Value: _TestValue = _TestValue()


def _Type_Not_Correct_Assertion_Error_Message(obj) -> str:
    return f"The object type is incorrect and it should be type of '{obj}'."


def _Value_Not_Correct_Assertion_Error_Message(value_meaning, current_value, expected_value) -> str:
    return f"The {value_meaning} value should be same as expected value {expected_value}', but it got {current_value}."


class TestInitModule(ZKTestSpec):

    @pytest.fixture(scope="function")
    def uit_object(self) -> MetaDataUtil:
        self._pytest_zk_client = KazooClient(hosts=Zookeeper_Hosts)
        self._pytest_zk_client.start()

        return MetaDataUtil(
            client=ZookeeperClient(hosts=Zookeeper_Hosts),
            converter=JsonStrConverter()
        )

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    @ZK.add_node_with_value_first(
        path_and_value={
            ZKNode.GROUP_STATE: _Testing_Value.group_state_data_str,
            ZKNode.NODE_STATE: _Testing_Value.node_state_data_str,
            ZKNode.TASK: _Testing_Value.task_data_str,
            ZKNode.HEARTBEAT: _Testing_Value.heartbeat_data_str
        })
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    def test__get_metadata_from_zookeeper(self, uit_object: MetaDataUtil):
        # # GroupState
        state = uit_object.get_metadata_from_zookeeper(path=_Testing_Value.group_state_zookeeper_path,
                                                       as_obj=GroupState)
        assert isinstance(state, GroupState), _Type_Not_Correct_Assertion_Error_Message(GroupState)
        assert state.total_crawler == _Total_Crawler_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_crawler", state.total_crawler, _Total_Crawler_Value)
        assert state.total_runner == _Runner_Crawler_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_runner", state.total_runner, _Runner_Crawler_Value)
        assert state.total_backup == _Backup_Crawler_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_backup", state.total_backup, _Backup_Crawler_Value)
        assert state.standby_id == _State_Standby_ID_Value, \
            _Value_Not_Correct_Assertion_Error_Message("standby_id", state.standby_id, _State_Standby_ID_Value)

        # # NodeState
        state = uit_object.get_metadata_from_zookeeper(path=_Testing_Value.node_state_zookeeper_path, as_obj=NodeState)
        assert isinstance(state, NodeState), _Type_Not_Correct_Assertion_Error_Message(NodeState)
        assert state.group == _Crawler_Group_Name_Value, \
            _Value_Not_Correct_Assertion_Error_Message("group", state.group, _Crawler_Group_Name_Value)
        assert state.role == _Crawler_Role_Value, \
            _Value_Not_Correct_Assertion_Error_Message("role", state.role, _Crawler_Role_Value)

        # # Task
        task = uit_object.get_metadata_from_zookeeper(path=_Testing_Value.task_zookeeper_path, as_obj=Task)
        assert isinstance(task, Task), _Type_Not_Correct_Assertion_Error_Message(Task)
        assert task.running_status == _Task_Running_State, \
            _Value_Not_Correct_Assertion_Error_Message("running_status", task.running_status, _Task_Running_State)
        assert task.running_content == [], \
            _Value_Not_Correct_Assertion_Error_Message("running_content",
                                                       task.running_content,
                                                       _Task_Running_Content_Value)

        # # Heartbeat
        heartbeat = uit_object.get_metadata_from_zookeeper(path=_Testing_Value.heartbeat_zookeeper_path,
                                                           as_obj=Heartbeat)
        assert isinstance(heartbeat, Heartbeat), _Type_Not_Correct_Assertion_Error_Message(Heartbeat)
        assert heartbeat.heart_rhythm_time == _Time_Value.strftime(_Time_Format_Value), \
            _Value_Not_Correct_Assertion_Error_Message("datetime of heartbeat",
                                                       heartbeat.heart_rhythm_time,
                                                       _Time_Value)

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    @ZK.create_node_first(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    def test__set_group_state_to_zookeeper(self, uit_object: MetaDataUtil):
        # # GroupState
        uit_object.set_metadata_to_zookeeper(path=_Testing_Value.group_state_zookeeper_path,
                                             metadata=_Testing_Value.group_state)
        state, znode_state = self._get_value_from_node(path=_Testing_Value.group_state_zookeeper_path)
        assert len(state) != 0, _Not_None_Assertion_Error

        # # NodeState
        uit_object.set_metadata_to_zookeeper(path=_Testing_Value.node_state_zookeeper_path,
                                             metadata=_Testing_Value.node_state)
        state, znode_state = self._get_value_from_node(path=_Testing_Value.node_state_zookeeper_path)
        assert len(state) != 0, _Not_None_Assertion_Error

        # # Task
        uit_object.set_metadata_to_zookeeper(path=_Testing_Value.task_zookeeper_path, metadata=_Testing_Value.task)
        task, znode_state = self._get_value_from_node(path=_Testing_Value.task_zookeeper_path)
        assert len(task) != 0, _Not_None_Assertion_Error

        # # Heartbeat
        uit_object.set_metadata_to_zookeeper(path=_Testing_Value.heartbeat_zookeeper_path,
                                             metadata=_Testing_Value.heartbeat)
        heartbeat, znode_state = self._get_value_from_node(path=_Testing_Value.heartbeat_zookeeper_path)
        assert len(heartbeat) != 0, _Not_None_Assertion_Error
