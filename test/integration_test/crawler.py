from smoothcrawler_cluster.model import Initial, Empty, Update, CrawlerStateRole, TaskResult, HeartState, GroupState, NodeState, Task, Heartbeat
from smoothcrawler_cluster.election import ElectionResult
from smoothcrawler_cluster.crawler import ZookeeperCrawler
from smoothcrawler_cluster.exceptions import ZookeeperCrawlerNotReady
from datetime import datetime
from kazoo.protocol.states import ZnodeStat
from kazoo.client import KazooClient
from typing import List, Dict
import traceback
import threading
import pytest
import json
import time

from ._zk_testsuite import ZK, ZKNode, ZKTestSpec
from .._config import Zookeeper_Hosts
from .._values import (
    # Crawler
    _Waiting_Time,
    # GroupState
    _Runner_Crawler_Value, _Backup_Crawler_Value, _Total_Crawler_Value, _Crawler_Role_Value, _State_Standby_ID_Value,
    # NodeState
    _Crawler_Group_Name_Value,
    # Task
    _Task_Running_Content_Value, _Task_Running_State,
    # Heartbeat
    _Time_Value, _Time_Format_Value,
    # common functions
    setup_group_state, setup_node_state, setup_task, setup_heartbeat
)
from .._components import RequestsHTTPRequest, RequestsHTTPResponseParser, ExampleWebDataHandler


def _Type_Not_Correct_Assertion_Error_Message(obj) -> str:
    return f"The object type is incorrect and it should be type of '{obj}'."


def _Value_Not_Correct_Assertion_Error_Message(value_meaning, current_value, expected_value) -> str:
    return f"The {value_meaning} value should be same as expected value {expected_value}', but it got {current_value}."


_Not_None_Assertion_Error: str = "It should not be None object."


# TODO: Consider that this class should be managed in a module or not
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
    def group_state_zk_path(self) -> str:
        if self.__Group_State_ZK_Path == "":
            self.__Group_State_ZK_Path = self._zk_client_inst.group_state_zookeeper_path
        return self.__Group_State_ZK_Path

    @property
    def node_state_zk_path(self) -> str:
        if self.__Node_State_ZK_Path == "":
            self.__Node_State_ZK_Path = self._zk_client_inst.node_state_zookeeper_path
        return self.__Node_State_ZK_Path

    @property
    def task_zk_path(self) -> str:
        if self.__Task_ZK_Path == "":
            self.__Task_ZK_Path = self._zk_client_inst.task_zookeeper_path
        return self.__Task_ZK_Path

    @property
    def heartbeat_zk_path(self) -> str:
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


_Testing_Value: _TestValue = _TestValue()


class TestZookeeperCrawler(ZKTestSpec):

    @pytest.fixture(scope="function")
    def uit_object(self) -> ZookeeperCrawler:
        self._PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
        self._PyTest_ZK_Client.start()

        return ZookeeperCrawler(runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, initial=False, zk_hosts=Zookeeper_Hosts)


    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.add_node_with_value_first(
        path_and_value={
            ZKNode.GroupState: _Testing_Value.group_state_data_str,
            ZKNode.NodeState: _Testing_Value.node_state_data_str,
            ZKNode.Task: _Testing_Value.task_data_str,
            ZKNode.Heartbeat: _Testing_Value.heartbeat_data_str
        })
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    def test__get_metadata_from_zookeeper(self, uit_object: ZookeeperCrawler):
        # # GroupState
        _state = uit_object._get_metadata_from_zookeeper(path=uit_object.group_state_zookeeper_path, as_obj=GroupState)
        assert type(_state) is GroupState, _Type_Not_Correct_Assertion_Error_Message(GroupState)
        assert _state.total_crawler == _Total_Crawler_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_crawler", _state.total_crawler, _Total_Crawler_Value)
        assert _state.total_runner == _Runner_Crawler_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_runner", _state.total_runner, _Runner_Crawler_Value)
        assert _state.total_backup == _Backup_Crawler_Value, \
            _Value_Not_Correct_Assertion_Error_Message("total_backup", _state.total_backup, _Backup_Crawler_Value)
        assert _state.standby_id == _State_Standby_ID_Value, \
            _Value_Not_Correct_Assertion_Error_Message("standby_id", _state.standby_id, _State_Standby_ID_Value)

        # # NodeState
        _state = uit_object._get_metadata_from_zookeeper(path=uit_object.node_state_zookeeper_path, as_obj=NodeState)
        assert type(_state) is NodeState, _Type_Not_Correct_Assertion_Error_Message(NodeState)
        assert _state.group == _Crawler_Group_Name_Value, \
            _Value_Not_Correct_Assertion_Error_Message("group", _state.group, _Crawler_Group_Name_Value)
        assert _state.role == _Crawler_Role_Value, \
            _Value_Not_Correct_Assertion_Error_Message("role", _state.role, _Crawler_Role_Value)

        # # Task
        _task = uit_object._get_metadata_from_zookeeper(path=uit_object.task_zookeeper_path, as_obj=Task)
        assert type(_task) is Task, _Type_Not_Correct_Assertion_Error_Message(Task)
        assert _task.running_status == _Task_Running_State, \
            _Value_Not_Correct_Assertion_Error_Message("running_status", _task.running_status, _Task_Running_State)
        assert _task.running_content == [], \
            _Value_Not_Correct_Assertion_Error_Message("running_content", _task.running_content, _Task_Running_Content_Value)

        # # Heartbeat
        _heartbeat = uit_object._get_metadata_from_zookeeper(path=uit_object.heartbeat_zookeeper_path, as_obj=Heartbeat)
        assert type(_heartbeat) is Heartbeat, _Type_Not_Correct_Assertion_Error_Message(Heartbeat)
        assert _heartbeat.heart_rhythm_time == _Time_Value.strftime(_Time_Format_Value), \
            _Value_Not_Correct_Assertion_Error_Message("datetime of heartbeat", _heartbeat.heart_rhythm_time, _Time_Value)


    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.create_node_first(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    def test__set_group_state_to_zookeeper(self, uit_object: ZookeeperCrawler):
        # # GroupState
        uit_object._set_metadata_to_zookeeper(path=uit_object.group_state_zookeeper_path, metadata=_Testing_Value.group_state)
        _state, _znode_state = self._get_zk_node_value(path=_Testing_Value.group_state_zk_path)
        assert type(_state) is not None, _Not_None_Assertion_Error

        # # NodeState
        uit_object._set_metadata_to_zookeeper(path=uit_object.node_state_zookeeper_path, metadata=_Testing_Value.node_state)
        _state, _znode_state = self._get_zk_node_value(path=_Testing_Value.node_state_zk_path)
        assert type(_state) is not None, _Not_None_Assertion_Error

        # # Task
        uit_object._set_metadata_to_zookeeper(path=uit_object.task_zookeeper_path, metadata=_Testing_Value.task)
        _task, _znode_state = self._get_zk_node_value(path=_Testing_Value.task_zk_path)
        assert type(_task) is not None, _Not_None_Assertion_Error

        # # Heartbeat
        uit_object._set_metadata_to_zookeeper(path=uit_object.heartbeat_zookeeper_path, metadata=_Testing_Value.heartbeat)
        _heartbeat, _znode_state = self._get_zk_node_value(path=_Testing_Value.heartbeat_zk_path)
        assert type(_heartbeat) is not None, _Not_None_Assertion_Error


    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.GroupState: _Testing_Value.group_state_data_str, ZKNode.NodeState: _Testing_Value.node_state_data_str})
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState])
    def test__update_crawler_role(self, uit_object: ZookeeperCrawler):
        # Checking initial state
        _group_state = uit_object._get_metadata_from_zookeeper(path=uit_object.group_state_zookeeper_path, as_obj=GroupState)
        assert len(_group_state.current_crawler) == 0, "At initial process, the length of current crawler list should be 0."
        assert len(_group_state.current_runner) == 0, "At initial process, the length of current runner list should be 0."
        assert len(_group_state.current_backup) == 0, "At initial process, the length of current backup list should be 0."

        _node_state = uit_object._get_metadata_from_zookeeper(path=uit_object.node_state_zookeeper_path, as_obj=NodeState)
        assert _node_state.role == CrawlerStateRole.Initial.value, \
            "At initial process, the role of crawler instance should be *initial* (or value of *CrawlerStateRole.Initial*)."

        # Checking initial state
        uit_object._update_crawler_role(CrawlerStateRole.Runner)

        # Verify the updated state
        _updated_group_state = uit_object._get_metadata_from_zookeeper(path=uit_object.group_state_zookeeper_path, as_obj=GroupState)
        assert len(_updated_group_state.current_runner) == 1, \
            "After update the *state* meta data, the length of current crawler list should be 1 because it's *runner*."
        assert len(_updated_group_state.current_backup) == 0, \
            "After update the *state* meta data, the length of current crawler list should be 0 because it's *runner*."

        _updated_node_state = uit_object._get_metadata_from_zookeeper(path=uit_object.node_state_zookeeper_path, as_obj=NodeState)
        assert _updated_node_state.role == CrawlerStateRole.Runner.value, \
            "After update the *state* meta data, its role should change to be *runner* (*CrawlerStateRole.Runner*)."


    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.Task, ZKNode.Heartbeat])
    def test_register_with_not_exist_node(self, uit_object: ZookeeperCrawler):
        self.__operate_register_and_verify_result(zk_crawler=uit_object)


    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.add_node_with_value_first(
        path_and_value={
            ZKNode.GroupState: _Testing_Value.group_state_data_str,
            ZKNode.NodeState: _Testing_Value.node_state_data_str,
            ZKNode.Task: _Testing_Value.task_data_str,
            ZKNode.Heartbeat: _Testing_Value.heartbeat_data_str
        })
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    def test_register_with_exist_node(self, uit_object: ZookeeperCrawler):
        self.__operate_register_and_verify_result(zk_crawler=uit_object)


    def __operate_register_and_verify_result(self, zk_crawler: ZookeeperCrawler):

        def _not_none_assertion(obj_type: str):
            return f"It should get something data and its data type is *{obj_type}*."

        # Operate target method to test
        zk_crawler.register()

        # Get the result which would be generated or modified by target method
        _group_state, _znode_state = self._get_zk_node_value(path=_Testing_Value.group_state_zk_path)
        _node_state, _znode_state = self._get_zk_node_value(path=_Testing_Value.node_state_zk_path)
        _task, _znode_state = self._get_zk_node_value(path=_Testing_Value.task_zk_path)
        _heartbeat, _znode_state = self._get_zk_node_value(path=_Testing_Value.heartbeat_zk_path)

        # Verify the values
        assert _group_state is not None, _not_none_assertion("GroupState")
        assert _node_state is not None, _not_none_assertion("NodeState")
        assert _task is not None, _not_none_assertion("Task")
        assert _heartbeat is not None, _not_none_assertion("Heartbeat")

        _assertion = "The value should be the same."
        _state_json = json.loads(_group_state)
        assert _state_json["total_crawler"] == _Total_Crawler_Value, _assertion
        assert _state_json["total_runner"] == _Runner_Crawler_Value, _assertion
        assert _state_json["total_backup"] == _Backup_Crawler_Value, _assertion
        assert _state_json["standby_id"] == "0", _assertion

        _node_state_json = json.loads(_node_state)
        assert _node_state_json["role"] == _Crawler_Role_Value, _assertion

        _task_json = json.loads(_task)
        assert _task_json["running_content"] == [], _assertion
        assert _task_json["running_status"] == _Task_Running_State, _assertion

        _heartbeat_json = json.loads(_heartbeat)
        assert _heartbeat_json["heart_rhythm_time"] is not None, _assertion


    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    def test_is_ready_with_positive_timeout(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        uit_object.register()
        _func_start_time = time.time()
        _result = uit_object.is_ready(timeout=_Waiting_Time)
        _func_end__time = time.time()

        # Verify the values
        assert _result is False, "It should be *False* because the property *current_crawler* size is still only 1."
        assert _Waiting_Time <= (_func_end__time - _func_start_time) < (_Waiting_Time + 1), \
            f"The function running time should be done about {_Waiting_Time} - {_Waiting_Time + 1} seconds."


    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    def test_elect(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        uit_object.register()
        _election_result = uit_object.elect()

        # Verify the values
        assert _election_result is ElectionResult.Winner, "It should be *ElectionResult.Winner* after the election with only one member."


    def _get_zk_node_value(self, path: str) -> (str, ZnodeStat):
        _data, _state = self._PyTest_ZK_Client.get(path=path)
        return _data.decode("utf-8"), _state


    def test_register_and_elect_with_many_crawler_instances(self):
        self._PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
        self._PyTest_ZK_Client.start()

        _running_flag: Dict[str, bool] = {}
        _group_state_path: str = ""
        _node_state_path: str = ""
        _index_sep_char: str = "_"
        _all_paths: List[str] = []

        _is_ready_flag: Dict[str, bool] = {}
        _election_results: Dict[str, ElectionResult] = {}

        def _run_and_test(_name):
            _zk_crawler = None
            try:
                # Instantiate ZookeeperCrawler
                _zk_crawler = ZookeeperCrawler(
                    runner=_Runner_Crawler_Value,
                    backup=_Backup_Crawler_Value,
                    name=_name,
                    initial=False,
                    zk_hosts=Zookeeper_Hosts
                )
                nonlocal _group_state_path
                if _group_state_path == "":
                    _group_state_path = _zk_crawler.group_state_zookeeper_path
                _all_paths.append(_group_state_path)
                nonlocal _node_state_path
                if _node_state_path == "":
                    _node_state_path = _zk_crawler.node_state_zookeeper_path
                _all_paths.append(_node_state_path)
                _all_paths.append(_zk_crawler.task_zookeeper_path)
                _all_paths.append(_zk_crawler.heartbeat_zookeeper_path)

                # Run target methods
                _zk_crawler.ensure_register = True
                _zk_crawler.ensure_timeout = 10
                _zk_crawler.ensure_wait = 0.5
                _zk_crawler.register()

                # Verify the running result
                _zk_crawler_ready = _zk_crawler.is_ready(timeout=10)
                _is_ready_flag[_name] = _zk_crawler_ready

                # Try to run election
                _elect_result = _zk_crawler.elect()
                _election_results[_name] = _elect_result
            except:
                _running_flag[_name] = False
                raise
            else:
                _running_flag[_name] = True

        try:
            # Run the target methods by multi-threads
            self._run_multi_threads(target_function=_run_and_test, index_sep_char=_index_sep_char)
            self._check_running_status(_running_flag)

            # Verify the running result by the value from Zookeeper
            _data, _state = self._PyTest_ZK_Client.get(path=_group_state_path)
            _json_data = json.loads(_data.decode("utf-8"))
            print(f"[DEBUG] _state_path: {_group_state_path}, _json_data: {_json_data}")
            self._check_current_crawler(_json_data, _running_flag)
            self._check_is_ready_flags(_is_ready_flag)
            self._check_election_results(_election_results, _index_sep_char)
        finally:
            self._delete_zk_nodes(_all_paths)

    def _run_multi_threads(self, target_function, index_sep_char: str) -> None:
        _threads = []
        for i in range(1, _Total_Crawler_Value + 1):
            _crawler_thread = threading.Thread(target=target_function, args=(f"sc-crawler{index_sep_char}{i}",))
            _crawler_thread.daemon = True
            _threads.append(_crawler_thread)

        for _thread in _threads:
            _thread.start()

        for _thread in _threads:
            _thread.join()

    def _check_running_status(self, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread gets any exception in running."

    def _check_current_crawler(self, json_data, running_flag: Dict[str, bool]) -> None:
        _current_crawler = json_data["current_crawler"]
        assert _current_crawler is not None, "Attribute *current_crawler* should NOT be None."
        assert type(_current_crawler) is list, "The data type of attribute *current_crawler* should NOT be list."
        assert len(_current_crawler) == _Total_Crawler_Value, \
            f"The size of attribute *current_crawler* should NOT be '{_Total_Crawler_Value}'."
        assert len(_current_crawler) == len(set(_current_crawler)), \
            "Attribute *current_crawler* should NOT have any element is repeated."
        for _crawler_name in running_flag.keys():
            assert _crawler_name in _current_crawler, \
                f"The element '{_crawler_name}' should be one of attribute *current_crawler*."

    def _check_is_ready_flags(self, is_ready_flag: Dict[str, bool]) -> None:
        assert len(is_ready_flag.keys()) == _Total_Crawler_Value, \
            f"The size of *is_ready* feature checksum should be {_Total_Crawler_Value}."
        assert False not in is_ready_flag, \
            "It should NOT exist any checksum element is False (it means crawler doesn't ready for running election)."

    def _check_election_results(self, election_results: Dict[str, ElectionResult], index_sep_char: str) -> None:
        assert len(election_results.keys()) == _Total_Crawler_Value, \
            f"The size of *elect* feature checksum should be {_Total_Crawler_Value}."
        for _crawler_name, _election_result in election_results.items():
            _crawler_index = int(_crawler_name.split(index_sep_char)[-1])
            if _crawler_index <= _Runner_Crawler_Value:
                assert _election_result is ElectionResult.Winner, f"The election result of '{_crawler_name}' should be *ElectionResult.Winner*."
            else:
                assert _election_result is ElectionResult.Loser, f"The election result of '{_crawler_name}' should be *ElectionResult.Loser*."

    def test_many_crawler_instances_with_initial(self):
        self._PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
        self._PyTest_ZK_Client.start()

        _zk_crawler_instances: List[ZookeeperCrawler] = []

        _running_flag: Dict[str, bool] = {}
        _group_state_path: str = ""
        _node_state_path: str = ""
        _index_sep_char: str = "_"
        _all_paths: List[str] = []

        _role_results: Dict[str, CrawlerStateRole] = {}

        def _run_and_test(_name):
            _zk_crawler = None
            try:
                # Instantiate ZookeeperCrawler
                _zk_crawler = ZookeeperCrawler(
                    runner=_Runner_Crawler_Value,
                    backup=_Backup_Crawler_Value,
                    name=_name,
                    initial=True,
                    ensure_initial=True,
                    ensure_timeout=10,
                    ensure_wait=0.5,
                    zk_hosts=Zookeeper_Hosts
                )

                _zk_crawler_instances.append(_zk_crawler)

                nonlocal _group_state_path
                if _group_state_path == "":
                    _group_state_path = _zk_crawler.group_state_zookeeper_path
                _all_paths.append(_group_state_path)
                nonlocal _node_state_path
                if _node_state_path == "":
                    _node_state_path = _zk_crawler.node_state_zookeeper_path
                _all_paths.append(_node_state_path)
                _all_paths.append(_zk_crawler.task_zookeeper_path)
                _all_paths.append(_zk_crawler.heartbeat_zookeeper_path)

                # Get the instance role of the crawler cluster
                _role_results[_name] = _zk_crawler.role
            except:
                _running_flag[_name] = False
                raise
            else:
                _running_flag[_name] = True

        try:
            # Run the target methods by multi-threads
            self._run_multi_threads(target_function=_run_and_test, index_sep_char=_index_sep_char)
            self._check_running_status(_running_flag)

            # Verify the heartbeat info
            _heartbeat_paths = filter(lambda _path: "heartbeat" in _path, _all_paths)
            self._check_heartbeat_info(list(_heartbeat_paths))

            # Verify the running result by the value from Zookeeper
            _data, _state = self._PyTest_ZK_Client.get(path=_group_state_path)
            _json_data = json.loads(_data.decode("utf-8"))
            print(f"[DEBUG] _state_path: {_group_state_path}, _json_data: {_json_data}")
            self._check_current_crawler(_json_data, _running_flag)
            self._check_current_runner(_json_data, _index_sep_char)
            self._check_current_backup_and_standby_id(_json_data, _index_sep_char)
            self._check_role(_role_results, _index_sep_char)
        finally:
            for _instance in _zk_crawler_instances:
                _instance.stop_update_heartbeat()
                del _instance
            time.sleep(5)    # Wait for kill instance of thread and ensure it's clear for all behind testing items
            self._delete_zk_nodes(_all_paths)

    def _check_heartbeat_info(self, heartbeat_paths: List[str]) -> None:
        for _path in heartbeat_paths:
            _task, _state = self._PyTest_ZK_Client.get(path=_path)
            print(f"[DEBUG] _path: {_path}, _task: {_task}")
            _task_json_data = json.loads(_task.decode("utf-8"))
            assert _task_json_data["time_format"] == "%Y-%m-%d %H:%M:%S", ""
            _heart_rhythm_time = _task_json_data["heart_rhythm_time"]
            _heart_rhythm_datetime = datetime.strptime(_heart_rhythm_time, _task_json_data["time_format"])
            _now_datetime = datetime.now()
            _diff_datetime = _now_datetime - _heart_rhythm_datetime
            assert _diff_datetime.total_seconds() < 2, ""
            assert _task_json_data["healthy_state"] == HeartState.Healthy.value, ""
            assert _task_json_data["task_state"] == TaskResult.Nothing.value, ""

    def _check_current_runner(self, json_data, index_sep_char: str) -> None:
        _current_runner = json_data["current_runner"]
        assert len(_current_runner) == _Runner_Crawler_Value, f"The size of attribute *current_runner* should be same as {_Runner_Crawler_Value}."
        _runer_checksum_list = list(
            map(lambda _crawler: int(_crawler.split(index_sep_char)[-1]) <= _Runner_Crawler_Value,
                _current_runner))
        assert False not in _runer_checksum_list, f"The index of all crawler name should be <= {_Runner_Crawler_Value} (the count of runner)."

    def _check_current_backup_and_standby_id(self, json_data, index_sep_char: str) -> None:
        _current_backup = json_data["current_backup"]
        assert len(_current_backup) == _Backup_Crawler_Value, f"The size of attribute *current_backup* should be same as {_Backup_Crawler_Value}."
        _backup_checksum_list = list(
            map(lambda _crawler: int(_crawler.split(index_sep_char)[-1]) > _Backup_Crawler_Value, _current_backup))
        assert False not in _backup_checksum_list, f"The index of all crawler name should be > {_Backup_Crawler_Value} (the count of runner)."

        _standby_id = json_data["standby_id"]
        _standby_id_checksum_list = list(map(lambda _crawler: _standby_id in _crawler, _current_backup))
        assert True in _standby_id_checksum_list, "The standby ID (the index of crawler name) should be included in the one name of backup crawlers list."

    def _check_role(self, role_results: Dict[str, CrawlerStateRole], index_sep_char: str) -> None:
        assert len(role_results.keys()) == _Total_Crawler_Value, \
            f"The size of *role* attribute checksum should be {_Total_Crawler_Value}."
        for _crawler_name, _role in role_results.items():
            _crawler_index = int(_crawler_name.split(index_sep_char)[-1])
            if _crawler_index <= _Runner_Crawler_Value:
                assert _role is CrawlerStateRole.Runner, f"The role of this crawler instance '{_crawler_name}' should be '{CrawlerStateRole.Runner}'."
            else:
                assert _role is CrawlerStateRole.Backup_Runner, f"The role of this crawler instance '{_crawler_name}' should be '{CrawlerStateRole.Backup_Runner}'."

    def _delete_zk_nodes(self, all_paths: List[str]) -> None:
        _sorted_all_paths = list(set(all_paths))
        for _path in _sorted_all_paths:
            if self._PyTest_ZK_Client.exists(path=_path) is not None:
                self._PyTest_ZK_Client.delete(path=_path)

    def test_wait_for_standby(self, uit_object: ZookeeperCrawler):
        # Prepare the meta data for testing this scenario
        # Set a *GroupState* with only 2 crawlers and standby ID is '1'
        _state = Initial.group_state(
            crawler_name="sc-crawler_0",
            total_crawler=3,
            total_runner=2,
            total_backup=1,
            standby_id="1",
            current_crawler=["sc-crawler_0", "sc-crawler_1", "sc-crawler_2"],
            current_runner=["sc-crawler_0"],
            current_backup=["sc-crawler_1"]
        )
        if self._exist_node(path=uit_object.group_state_zookeeper_path) is None:
            _state_data_str = json.dumps(_state.to_readable_object())
            self._create_node(path=uit_object.group_state_zookeeper_path, value=bytes(_state_data_str, "utf-8"), include_data=True)

        # Set a *NodeState* of sc-crawler_0
        _node_state = Initial.node_state(group=uit_object._crawler_group, role=CrawlerStateRole.Runner)
        _crawler_0_node_state_path = uit_object.node_state_zookeeper_path.replace("1", "0")
        if self._exist_node(path=_crawler_0_node_state_path) is None:
            _node_state_data_str = json.dumps(_node_state.to_readable_object())
            self._create_node(path=_crawler_0_node_state_path, value=bytes(_node_state_data_str, "utf-8"), include_data=True)

        # Set a *Task* of sc-crawler_0
        _task = Initial.task(running_state=TaskResult.Processing)
        _crawler_0_task_path = uit_object.task_zookeeper_path.replace("1", "0")
        if self._exist_node(path=_crawler_0_task_path) is None:
            _task_data_str = json.dumps(_task.to_readable_object())
            self._create_node(path=_crawler_0_task_path, value=bytes(_task_data_str, "utf-8"), include_data=True)

        # Set a *Heartbeat* of sc-crawler_0 is dead
        _heartbeat = Initial.heartbeat()
        _crawler_0_heartbeat_path = uit_object.heartbeat_zookeeper_path.replace("1", "0")
        if self._exist_node(path=_crawler_0_heartbeat_path) is None:
            _heartbeat_data_str = json.dumps(_heartbeat.to_readable_object())
            self._create_node(path=_crawler_0_heartbeat_path, value=bytes(_heartbeat_data_str, "utf-8"), include_data=True)

        # Set a *NodeState* of sc-crawler_1
        _node_state = Initial.node_state(group=uit_object._crawler_group, role=CrawlerStateRole.Runner)
        _crawler_1_node_state_path = uit_object.node_state_zookeeper_path
        if self._exist_node(path=_crawler_1_node_state_path) is None:
            _node_state_data_str = json.dumps(_node_state.to_readable_object())
            self._create_node(path=_crawler_1_node_state_path, value=bytes(_node_state_data_str, "utf-8"), include_data=True)

        # Run the function which target to test
        try:
            uit_object.wait_and_standby()
        except ZookeeperCrawlerNotReady:
            # assert str(e) == "Current crawler instance is not ready for running. Its *current_runner* still be empty.", ""
            assert True, "It works finely!"
        else:
            assert False, "It should raise an exception about ZookeeperCrawler is not ready to run."

        try:
            if uit_object.is_ready(timeout=5):
                uit_object.wait_and_standby()
            else:
                assert False, "It should be ready to run. Please check the detail implementation or other settings in testing has problem or not."

            # Verify the result should be correct as expected
            # Verify the *GroupState* info
            _data, _state = self._get_value_from_node(path=uit_object.group_state_zookeeper_path)
            _json_data = json.loads(str(_data.decode("utf-8")))
            print(f"[DEBUG in testing] _json_data: {_json_data}")
            assert _json_data["standby_id"] == "2", ""

            assert len(_json_data["current_crawler"]) == 3, ""
            assert uit_object._crawler_name in _json_data["current_crawler"], ""
            assert len(_json_data["current_runner"]) == 1, ""
            assert uit_object._crawler_name in _json_data["current_runner"], ""
            assert len(_json_data["current_backup"]) == 0, ""

            assert len(_json_data["fail_crawler"]) == 1, ""
            assert _json_data["fail_crawler"][0] == "sc-crawler_0", ""
            assert len(_json_data["fail_runner"]) == 1, ""
            assert _json_data["fail_runner"][0] == "sc-crawler_0", ""
            assert len(_json_data["fail_backup"]) == 0, ""

            # Verify the *NodeState* info
            _node_data, _state = self._get_value_from_node(path=uit_object.node_state_zookeeper_path)
            _json_node_data = json.loads(str(_node_data.decode("utf-8")))
            print(f"[DEBUG in testing] _json_node_data: {_json_node_data}")
            assert _json_node_data["group"] == uit_object._crawler_group, ""
            assert _json_node_data["role"] == CrawlerStateRole.Runner.value, ""

            _0_node_data, _state = self._get_value_from_node(path=uit_object.node_state_zookeeper_path.replace("1", "0"))
            _json_0_node_data = json.loads(str(_0_node_data.decode("utf-8")))
            print(f"[DEBUG in testing] _json_0_node_data: {_json_0_node_data}")
            assert _json_0_node_data["group"] == uit_object._crawler_group, ""
            assert _json_0_node_data["role"] == CrawlerStateRole.Dead_Runner.value, ""
        finally:
            if self._exist_node(path=uit_object.group_state_zookeeper_path):
                self._delete_node(path=uit_object.group_state_zookeeper_path)
            if self._exist_node(path=uit_object.node_state_zookeeper_path):
                self._delete_node(path=uit_object.node_state_zookeeper_path)
            if self._exist_node(path=uit_object.task_zookeeper_path):
                self._delete_node(path=uit_object.task_zookeeper_path)
            if self._exist_node(path=uit_object.heartbeat_zookeeper_path):
                self._delete_node(path=uit_object.heartbeat_zookeeper_path)
            if self._exist_node(path=_crawler_0_task_path):
                self._delete_node(path=_crawler_0_task_path)
            if self._exist_node(path=_crawler_0_heartbeat_path):
                self._delete_node(path=_crawler_0_heartbeat_path)

    def test_wait_for_to_be_standby(self):
        self._PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
        self._PyTest_ZK_Client.start()

        # # Prepare the meta data
        # Instantiate a ZookeeperCrawler for testing
        _zk_crawler = ZookeeperCrawler(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            name="sc-crawler_2",
            initial=False,
            zk_hosts=Zookeeper_Hosts
        )

        # Set a *State* with only 2 crawlers and standby ID is '1'
        _state = Initial.group_state(
            crawler_name="sc-crawler_0",
            total_crawler=3,
            total_runner=2,
            total_backup=1,
            standby_id="1",
            current_crawler=["sc-crawler_0", "sc-crawler_1", "sc-crawler_2"],
            current_runner=["sc-crawler_0"],
            current_backup=["sc-crawler_1", "sc-crawler_2"]
        )
        if self._exist_node(path=_zk_crawler.group_state_zookeeper_path) is None:
            _state_data_str = json.dumps(_state.to_readable_object())
            self._create_node(path=_zk_crawler.group_state_zookeeper_path, value=bytes(_state_data_str, "utf-8"), include_data=True)

        _result = None
        _start = None
        _end = None

        def _update_state_standby_id():
            time.sleep(5)
            _state.standby_id = "2"
            _new_state_data_str = json.dumps(_state.to_readable_object())
            self._set_value_to_node(path=_zk_crawler.group_state_zookeeper_path, value=bytes(_new_state_data_str, "utf-8"))

        def _run_target_test_func():
            try:
                nonlocal _result, _start, _end
                _start = time.time()
                # # Run target function
                _result = _zk_crawler.wait_for_to_be_standby()
                _end = time.time()
            finally:
                if self._exist_node(path=_zk_crawler.group_state_zookeeper_path):
                    self._delete_node(path=_zk_crawler.group_state_zookeeper_path)

        _updating_thread = threading.Thread(target=_update_state_standby_id)
        _run_target_thread = threading.Thread(target=_run_target_test_func)

        _updating_thread.start()
        _run_target_thread.start()

        _updating_thread.join()
        _run_target_thread.join()

        # # Verify the result
        assert _result is True, "It should be True after it detect the stand ID to be '2'."
        assert 5 < int(_end - _start) <= 6, "It should NOT run more than 6 seconds."

    def test_run_task(self, uit_object: ZookeeperCrawler):
        _empty_task = Empty.task()
        if self._exist_node(path=uit_object.task_zookeeper_path) is None:
            _task_data_str = json.dumps(_empty_task.to_readable_object())
            self._create_node(path=uit_object.task_zookeeper_path, value=bytes(_task_data_str, "utf-8"), include_data=True)

        def _assign_task() -> None:
            time.sleep(5)
            _task = uit_object._get_metadata_from_zookeeper(path=uit_object.task_zookeeper_path, as_obj=Task)
            _updated_task = Update.task(_task, running_content=_Task_Running_Content_Value)
            _updated_task_str = json.dumps(_updated_task.to_readable_object())
            self._set_value_to_node(path=uit_object.task_zookeeper_path, value=bytes(_updated_task_str, "utf-8"))

        def _wait_for_task() -> None:
            try:
                uit_object.wait_for_task()
            except NotImplementedError:
                assert True, "It works finely."
            else:
                assert False, "It should raise an error about *NotImplementedError* of registering SmoothCrawler components."

            try:
                uit_object.register_factory(
                    http_req_sender=RequestsHTTPRequest(),
                    http_resp_parser=RequestsHTTPResponseParser(),
                    data_process=ExampleWebDataHandler()
                )
                uit_object.wait_for_task()
            except Exception:
                assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
            else:
                assert True, "It works finely."

        try:
            _assign_task_thread = threading.Thread(target=_assign_task)
            _run_task_thread = threading.Thread(target=_wait_for_task)
            _run_task_thread.daemon = True

            _assign_task_thread.start()
            _run_task_thread.start()

            _assign_task_thread.join()

            time.sleep(3)

            # Verify the running result
            _task_data, state = self._get_value_from_node(path=uit_object.task_zookeeper_path)
            _json_task_data = json.loads(str(_task_data.decode("utf-8")))
            print(f"[DEBUG in testing] _json_task_data: {_json_task_data}")
            assert _json_task_data["in_progressing_id"] == "-1", "It should be reset as '-1' finally in crawl processing."
            assert _json_task_data["running_result"] == {"success_count": 1, "fail_count": 0}, "It should has 1 successful and 0 fail result."
            assert _json_task_data["running_status"] == TaskResult.Done.value, "It should be 'TaskResult.Done' state."
            assert len(_json_task_data["result_detail"]) == 1, "It's size should be 1."
            assert _json_task_data["result_detail"][0] == {
                "task_id": _Task_Running_Content_Value[0]["task_id"],
                "state": TaskResult.Done.value,
                "status_code": 200,
                "response": "Example Domain",
                "error_msg": None
            }, "The detail should be completely same as above."
        finally:
            _under_test_task_path = uit_object.task_zookeeper_path
            del uit_object
            time.sleep(5)    # Wait for kill instance of thread and ensure it's clear for all behind testing items
            if self._exist_node(path=_under_test_task_path):
                self._delete_node(path=_under_test_task_path)

