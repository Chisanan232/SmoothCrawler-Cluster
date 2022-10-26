from smoothcrawler_cluster.model.metadata import State, Task, Heartbeat
from smoothcrawler_cluster.model.metadata_enum import CrawlerStateRole, TaskResult
from smoothcrawler_cluster.election import ElectionResult
from smoothcrawler_cluster.crawler import ZookeeperCrawler
from kazoo.protocol.states import ZnodeStat
from kazoo.client import KazooClient
from datetime import datetime
from typing import TypeVar
import pytest
import json
import time

from ._zk_testsuite import ZK, ZKTestSpec
from .._config import Zookeeper_Hosts


_Runner_Value: int = 2
_Backup_Value: int = 1

_ZookeeperCrawlerType = TypeVar("_ZookeeperCrawlerType", bound=ZookeeperCrawler)

_Expected_State: State = None
_Expected_Task: Task = None
_Expected_Heartbeat: Heartbeat = None

_State_Role_Value: CrawlerStateRole = CrawlerStateRole.Initial
_State_Total_Crawler_Value: int = 3
_State_Total_Runner_Value: int = 2
_State_Total_Backup_Value: int = 1
_State_Standby_ID_Value: str = "0"
_State_List_Value: list = []

_Task_Result_Value: TaskResult = TaskResult.Nothing
_Task_Content_Value: dict = {}

_Heartbeat_Value: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

_Waiting_Time: int = 5


def _Type_Not_Correct_Assertion_Error_Message(obj) -> str:
    return f"The object type is incorrect and it should be type of '{obj}'."


def _Value_Not_Correct_Assertion_Error_Message(value_meaning, current_value, expected_value) -> str:
    return f"The {value_meaning} value should be same as expected value {expected_value}', but it got {current_value}."


_Not_None_Assertion_Error: str = "It should not be None object."


class _TestValue:

    __Test_Value_Instance = None

    _State_ZK_Path: str = ""
    _Task_ZK_Path: str = ""
    _Heartbeat_ZK_Path: str = ""

    _Testing_State_Data_Str: str = ""
    _Testing_Task_Data_Str: str = ""
    _Testing_Heartbeat_Data_Str: str = ""

    __Testing_State: State = None
    __Testing_Task: Task = None
    __Testing_Heartbeat: Heartbeat = None

    def __new__(cls, *args, **kwargs):
        if cls.__Test_Value_Instance is None:
            cls.__Test_Value_Instance = super(_TestValue, cls).__new__(cls, *args, **kwargs)
        return cls.__Test_Value_Instance

    def __init__(self):
        self._zk_client_inst = ZookeeperCrawler(runner=_Runner_Value, backup=_Backup_Value, initial=False)

    @property
    def state_zk_path(self) -> str:
        if self._Task_ZK_Path == "":
            self._Task_ZK_Path = self._zk_client_inst.state_zookeeper_path
        return self._Task_ZK_Path

    @property
    def task_zk_path(self) -> str:
        if self._State_ZK_Path == "":
            self._State_ZK_Path = self._zk_client_inst.task_zookeeper_path
        return self._State_ZK_Path

    @property
    def heartbeat_zk_path(self) -> str:
        if self._Heartbeat_ZK_Path == "":
            self._Heartbeat_ZK_Path = self._zk_client_inst.heartbeat_zookeeper_path
        return self._Heartbeat_ZK_Path

    @property
    def state(self) -> State:
        if self.__Testing_State is None:
            _state = State()
            _state.role = _State_Role_Value
            _state.total_crawler = _State_Total_Crawler_Value
            _state.total_runner = _State_Total_Runner_Value
            _state.total_backup = _State_Total_Backup_Value
            _state.standby_id = _State_Standby_ID_Value
            _state.current_crawler = _State_List_Value
            _state.current_runner = _State_List_Value
            _state.current_backup = _State_List_Value
            _state.fail_crawler = _State_List_Value
            _state.fail_runner = _State_List_Value
            _state.fail_backup = _State_List_Value
            self.__Testing_State = _state

        global _Expected_State
        _Expected_State = self.__Testing_State

        return self.__Testing_State

    @property
    def task(self) -> Task:
        if self.__Testing_Task is None:
            _task = Task()
            _task.task_result = _Task_Result_Value
            _task.task_content = _Task_Content_Value
            self.__Testing_Task = _task

        global _Expected_Task
        _Expected_Task = self.__Testing_Task

        return self.__Testing_Task

    @property
    def heartbeat(self) -> Heartbeat:
        if self.__Testing_Heartbeat is None:
            _heartbeat = Heartbeat()
            _heartbeat.datetime = _Heartbeat_Value
            self.__Testing_Heartbeat = _heartbeat

        global _Expected_Heartbeat
        _Expected_Heartbeat = self.__Testing_Heartbeat

        return self.__Testing_Heartbeat

    @property
    def state_data_str(self) -> str:
        if self._Testing_State_Data_Str == "":
            self._Testing_State_Data_Str = json.dumps(self.state.to_readable_object())
        return self._Testing_State_Data_Str

    @property
    def task_data_str(self) -> str:
        if self._Testing_Task_Data_Str == "":
            self._Testing_Task_Data_Str = json.dumps(self.task.to_readable_object())
        return self._Testing_Task_Data_Str

    @property
    def heartbeat_data_str(self) -> str:
        if self._Testing_Heartbeat_Data_Str == "":
            self._Testing_Heartbeat_Data_Str = json.dumps(self.heartbeat.to_readable_object())
        return self._Testing_Heartbeat_Data_Str


_Testing_Value: _TestValue = _TestValue()


class TestZookeeperCrawler(ZKTestSpec):

    @pytest.fixture(scope="function")
    def uit_object(self) -> ZookeeperCrawler:
        self._PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
        self._PyTest_ZK_Client.start()

        _zk_crawler = ZookeeperCrawler(runner=_Runner_Value, backup=_Backup_Value, initial=False)
        return _zk_crawler


    @ZK.reset_testing_env(path=_Testing_Value.state_zk_path)
    @ZK.add_node_with_value_first(path=_Testing_Value.state_zk_path, value=_Testing_Value.state_data_str)
    @ZK.remove_node_finally(path=_Testing_Value.state_zk_path)
    def test__get_state_from_zookeeper(self, uit_object: ZookeeperCrawler):
        _state = uit_object._get_state_from_zookeeper()
        assert type(_state) is State, _Type_Not_Correct_Assertion_Error_Message(State)
        assert _state.role == _State_Role_Value.value, \
            _Value_Not_Correct_Assertion_Error_Message("role", _state.role, _State_Role_Value)
        assert _state.total_crawler == _State_Total_Crawler_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_crawler", _state.total_crawler, _State_Total_Crawler_Value)
        assert _state.total_runner == _State_Total_Runner_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_runner", _state.total_runner, _State_Total_Runner_Value)
        assert _state.total_backup == _State_Total_Backup_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_backup", _state.total_backup, _State_Total_Backup_Value)
        assert _state.standby_id == _State_Standby_ID_Value, \
            _Value_Not_Correct_Assertion_Error_Message("standby_id", _state.standby_id, _State_Standby_ID_Value)


    @ZK.reset_testing_env(path=_Testing_Value.task_zk_path)
    @ZK.add_node_with_value_first(path=_Testing_Value.task_zk_path, value=_Testing_Value.task_data_str)
    @ZK.remove_node_finally(path=_Testing_Value.task_zk_path)
    def test__get_task_from_zookeeper(self, uit_object: ZookeeperCrawler):
        _task = uit_object._get_task_from_zookeeper()
        assert type(_task) is Task, _Type_Not_Correct_Assertion_Error_Message(Task)
        assert _task.task_result == _Task_Result_Value.value, \
            _Value_Not_Correct_Assertion_Error_Message("task_result", _task.task_result, _Task_Result_Value)
        assert _task.task_content == _Task_Content_Value, \
            _Value_Not_Correct_Assertion_Error_Message("task_content", _task.task_content, _Task_Content_Value)


    @ZK.reset_testing_env(path=_Testing_Value.heartbeat_zk_path)
    @ZK.add_node_with_value_first(path=_Testing_Value.heartbeat_zk_path, value=_Testing_Value.heartbeat_data_str)
    @ZK.remove_node_finally(path=_Testing_Value.heartbeat_zk_path)
    def test__get_heartbeat_from_zookeeper(self, uit_object: ZookeeperCrawler):
        _heartbeat = uit_object._get_heartbeat_from_zookeeper()
        assert type(_heartbeat) is Heartbeat, _Type_Not_Correct_Assertion_Error_Message(Heartbeat)
        assert _heartbeat.datetime == _Heartbeat_Value, \
            _Value_Not_Correct_Assertion_Error_Message("datetime of heartbeat", _heartbeat.datetime, _Heartbeat_Value)


    @ZK.reset_testing_env(path=_Testing_Value.state_zk_path)
    @ZK.create_node_first(path=_Testing_Value.state_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.state_zk_path)
    def test__set_state_to_zookeeper(self, uit_object: ZookeeperCrawler):
        uit_object._set_state_to_zookeeper(state=_Testing_Value.state)
        _state, _znode_state = self._get_zk_node_value(path=_Testing_Value.state_zk_path)
        assert type(_state) is not None, _Not_None_Assertion_Error


    @ZK.reset_testing_env(path=_Testing_Value.task_zk_path)
    @ZK.create_node_first(path=_Testing_Value.task_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.task_zk_path)
    def test__set_task_to_zookeeper(self, uit_object: ZookeeperCrawler):
        uit_object._set_task_to_zookeeper(task=_Testing_Value.task)
        _task, _znode_state = self._get_zk_node_value(path=_Testing_Value.task_zk_path)
        assert type(_task) is not None, _Not_None_Assertion_Error


    @ZK.reset_testing_env(path=_Testing_Value.heartbeat_zk_path)
    @ZK.create_node_first(path=_Testing_Value.heartbeat_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.heartbeat_zk_path)
    def test__set_heartbeat_to_zookeeper(self, uit_object: ZookeeperCrawler):
        uit_object._set_heartbeat_to_zookeeper(heartbeat=_Testing_Value.heartbeat)
        _heartbeat, _znode_state = self._get_zk_node_value(path=_Testing_Value.heartbeat_zk_path)
        assert type(_heartbeat) is not None, _Not_None_Assertion_Error


    @ZK.reset_testing_env(path=_Testing_Value.state_zk_path)
    @ZK.add_node_with_value_first(path=_Testing_Value.state_zk_path, value=_Testing_Value.state_data_str)
    @ZK.remove_node_finally(path=_Testing_Value.state_zk_path)
    def test__update_crawler_role(self, uit_object: ZookeeperCrawler):
        # Checking initial state
        _state = uit_object._get_state_from_zookeeper()
        assert _state.role == CrawlerStateRole.Initial.value, \
            "At initial process, the role of crawler instance should be *initial* (or value of *CrawlerStateRole.Initial*)."
        assert len(_state.current_crawler) == 0, "At initial process, the length of current crawler list should be 0."
        assert len(_state.current_runner) == 0, "At initial process, the length of current runner list should be 0."
        assert len(_state.current_backup) == 0, "At initial process, the length of current backup list should be 0."

        # Checking initial state
        uit_object._update_crawler_role(CrawlerStateRole.Runner)

        # Verify the updated state
        _state = uit_object._get_state_from_zookeeper()
        assert _state.role == CrawlerStateRole.Runner.value, \
            "After update the *state* meta data, its role should change to be *runner* (*CrawlerStateRole.Runner*)."
        assert len(_state.current_crawler) == 1, \
            "After update the *state* meta data, the length of current crawler list should be 1 because runner election done."
        assert len(_state.current_runner) == 1, \
            "After update the *state* meta data, the length of current crawler list should be 1 because it's *runner*."
        assert len(_state.current_backup) == 0, \
            "After update the *state* meta data, the length of current crawler list should be 0 because it's *runner*."


    @ZK.reset_testing_env(path=_Testing_Value.state_zk_path)
    @ZK.reset_testing_env(path=_Testing_Value.task_zk_path)
    @ZK.reset_testing_env(path=_Testing_Value.heartbeat_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.state_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.task_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.heartbeat_zk_path)
    def test_register_with_not_exist_node(self, uit_object: ZookeeperCrawler):
        self.__operate_register_and_verify_result(zk_crawler=uit_object)


    @ZK.reset_testing_env(path=_Testing_Value.state_zk_path)
    @ZK.reset_testing_env(path=_Testing_Value.task_zk_path)
    @ZK.reset_testing_env(path=_Testing_Value.heartbeat_zk_path)
    @ZK.add_node_with_value_first(path=_Testing_Value.state_zk_path, value=_Testing_Value.state_data_str)
    @ZK.add_node_with_value_first(path=_Testing_Value.task_zk_path, value=_Testing_Value.task_data_str)
    @ZK.add_node_with_value_first(path=_Testing_Value.heartbeat_zk_path, value=_Testing_Value.heartbeat_data_str)
    @ZK.remove_node_finally(path=_Testing_Value.state_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.task_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.heartbeat_zk_path)
    def test_register_with_exist_node(self, uit_object: ZookeeperCrawler):
        self.__operate_register_and_verify_result(zk_crawler=uit_object)


    def __operate_register_and_verify_result(self, zk_crawler: ZookeeperCrawler):

        def _not_none_assertion(obj_type: str):
            return f"It should get something data and its data type is *{obj_type}*."

        # Operate target method to test
        zk_crawler.register()

        # Get the result which would be generated or modified by target method
        _state, _znode_state = self._get_zk_node_value(path=_Testing_Value.state_zk_path)
        _task, _znode_state = self._get_zk_node_value(path=_Testing_Value.task_zk_path)
        _heartbeat, _znode_state = self._get_zk_node_value(path=_Testing_Value.heartbeat_zk_path)

        # Verify the values
        assert _state is not None, _not_none_assertion("State")
        assert _task is not None, _not_none_assertion("Task")
        assert _heartbeat is not None, _not_none_assertion("Heartbeat")

        _assertion = "The value should be the same."
        _state_json = json.loads(_state)
        assert _state_json["role"] == _State_Role_Value.value, _assertion
        assert _state_json["total_crawler"] == _State_Total_Crawler_Value, _assertion
        assert _state_json["total_runner"] == _State_Total_Runner_Value, _assertion
        assert _state_json["total_backup"] == _State_Total_Backup_Value, _assertion
        assert _state_json["standby_id"] == _State_Standby_ID_Value, _assertion

        _task_json = json.loads(_task)
        assert _task_json["task_result"] == _Task_Result_Value.value, _assertion
        assert _task_json["task_content"] == {}, _assertion

        _heartbeat_json = json.loads(_heartbeat)
        assert _heartbeat_json["datetime"] is not None, _assertion


    @ZK.reset_testing_env(path=_Testing_Value.state_zk_path)
    @ZK.reset_testing_env(path=_Testing_Value.task_zk_path)
    @ZK.reset_testing_env(path=_Testing_Value.heartbeat_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.state_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.task_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.heartbeat_zk_path)
    def test_is_ready_with_positive_timeout(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        uit_object.register()
        _func_start_time = time.time()
        _result = uit_object.is_ready(timeout=_Waiting_Time)
        _func_end__time = time.time()

        # Verify the values
        assert _result is False, "It should be *False* because the property *current_crawler* size is still only 1."
        assert _Waiting_Time <= (_func_end__time - _func_start_time) < (_Waiting_Time + 1), \
            f"The function running time should be done about {_Waiting_Time} - {Waiting_Time + 1} seconds."


    @ZK.reset_testing_env(path=_Testing_Value.state_zk_path)
    @ZK.reset_testing_env(path=_Testing_Value.task_zk_path)
    @ZK.reset_testing_env(path=_Testing_Value.heartbeat_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.state_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.task_zk_path)
    @ZK.remove_node_finally(path=_Testing_Value.heartbeat_zk_path)
    def test_elect(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        uit_object.register()
        _election_result = uit_object.elect()

        # Verify the values
        assert _election_result is ElectionResult.Winner, "It should be *ElectionResult.Winner* after the election with only one member."


    def _get_zk_node_value(self, path: str) -> (str, ZnodeStat):

        def _get_value() -> (bytes, ZnodeStat):
            __data = None
            __state = None

            @self._PyTest_ZK_Client.DataWatch(path)
            def _get_value_from_path(data: bytes, state: ZnodeStat):
                nonlocal __data, __state
                __data = data
                __state = state

            return __data, __state

        _data, _state = _get_value()
        return _data.decode("utf-8"), _state

