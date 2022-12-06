from smoothcrawler_cluster.model import Initial, Empty, Update, CrawlerStateRole, TaskResult, HeartState, GroupState, NodeState, Task
from smoothcrawler_cluster.election import ElectionResult
from smoothcrawler_cluster.crawler import ZookeeperCrawler
from datetime import datetime
from kazoo.client import KazooClient
from typing import List, Dict
import multiprocessing as mp
import traceback
import threading
import pytest
import json
import time
import re

from ._instance_value import _TestValue, _ZKNodePathUtils
from ._zk_testsuite import ZK, ZKNode, ZKTestSpec
from .._config import Zookeeper_Hosts
from .._values import (
    # Crawler
    _Waiting_Time,
    # GroupState
    _Runner_Crawler_Value, _Backup_Crawler_Value, _Fail_Runner_Crawler_Value, _Total_Crawler_Value, _Crawler_Role_Value,
    # Task
    _One_Running_Content, _Task_Running_Content_Value, _Task_Running_State
)
from .._sample_components._components import RequestsHTTPRequest, RequestsHTTPResponseParser, ExampleWebDataHandler


_Manager = mp.Manager()
_ZK_Crawler_Instances: List[ZookeeperCrawler] = _Manager.list()
_Global_Exception_Record: Exception = None
_Testing_Value: _TestValue = _TestValue()


class TestZookeeperCrawlerSingleMajorFeature(ZKTestSpec):

    @pytest.fixture(scope="function")
    def uit_object(self) -> ZookeeperCrawler:
        self._PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
        self._PyTest_ZK_Client.start()

        return ZookeeperCrawler(runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, initial=False, zk_hosts=Zookeeper_Hosts)

    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.GroupState: _Testing_Value.group_state_data_str, ZKNode.NodeState: _Testing_Value.node_state_data_str})
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState])
    def test__update_crawler_role(self, uit_object: ZookeeperCrawler):
        # Checking initial state
        _group_state = uit_object._MetaData_Util.get_metadata_from_zookeeper(path=uit_object.group_state_zookeeper_path, as_obj=GroupState)
        assert len(_group_state.current_crawler) == 0, "At initial process, the length of current crawler list should be 0."
        assert len(_group_state.current_runner) == 0, "At initial process, the length of current runner list should be 0."
        assert len(_group_state.current_backup) == 0, "At initial process, the length of current backup list should be 0."

        _node_state = uit_object._MetaData_Util.get_metadata_from_zookeeper(path=uit_object.node_state_zookeeper_path, as_obj=NodeState)
        assert _node_state.role == CrawlerStateRole.Initial.value, \
            "At initial process, the role of crawler instance should be *initial* (or value of *CrawlerStateRole.Initial*)."

        # Checking initial state
        uit_object._update_crawler_role(CrawlerStateRole.Runner)

        # Verify the updated state
        _updated_group_state = uit_object._MetaData_Util.get_metadata_from_zookeeper(path=uit_object.group_state_zookeeper_path, as_obj=GroupState)
        assert len(_updated_group_state.current_runner) == 1, \
            "After update the *state* meta data, the length of current crawler list should be 1 because it's *runner*."
        assert len(_updated_group_state.current_backup) == 0, \
            "After update the *state* meta data, the length of current crawler list should be 0 because it's *runner*."

        _updated_node_state = uit_object._MetaData_Util.get_metadata_from_zookeeper(path=uit_object.node_state_zookeeper_path, as_obj=NodeState)
        assert _updated_node_state.role == CrawlerStateRole.Runner.value, \
            "After update the *state* meta data, its role should change to be *runner* (*CrawlerStateRole.Runner*)."

    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.Task, ZKNode.Heartbeat])
    def test_register_metadata_with_not_exist_node(self, uit_object: ZookeeperCrawler):
        self.__operate_register_metadata_and_verify_result(zk_crawler=uit_object)

    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.add_node_with_value_first(
        path_and_value={
            ZKNode.GroupState: _Testing_Value.group_state_data_str,
            ZKNode.NodeState: _Testing_Value.node_state_data_str,
            ZKNode.Task: _Testing_Value.task_data_str,
            ZKNode.Heartbeat: _Testing_Value.heartbeat_data_str
        })
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    def test_register_metadata_with_exist_node(self, uit_object: ZookeeperCrawler):
        self.__operate_register_metadata_and_verify_result(zk_crawler=uit_object)

    def __operate_register_metadata_and_verify_result(self, zk_crawler: ZookeeperCrawler):

        def _not_none_assertion(obj_type: str):
            return f"It should get something data and its data type is *{obj_type}*."

        # Operate target method to test
        zk_crawler.register_metadata()

        # Get the result which would be generated or modified by target method
        _group_state, _znode_state = self._get_value_from_node(path=_Testing_Value.group_state_zookeeper_path)
        _node_state, _znode_state = self._get_value_from_node(path=_Testing_Value.node_state_zookeeper_path)
        _task, _znode_state = self._get_value_from_node(path=_Testing_Value.task_zookeeper_path)
        _heartbeat, _znode_state = self._get_value_from_node(path=_Testing_Value.heartbeat_zookeeper_path)

        # Verify the values
        assert _group_state is not None, _not_none_assertion("GroupState")
        assert _node_state is not None, _not_none_assertion("NodeState")
        assert _task is not None, _not_none_assertion("Task")
        assert _heartbeat is not None, _not_none_assertion("Heartbeat")

        _assertion = "The value should be the same."
        _state_json = json.loads(_group_state.decode("utf-8"))
        assert _state_json["total_crawler"] == _Total_Crawler_Value, _assertion
        assert _state_json["total_runner"] == _Runner_Crawler_Value, _assertion
        assert _state_json["total_backup"] == _Backup_Crawler_Value, _assertion
        assert _state_json["standby_id"] == "0", _assertion

        _node_state_json = json.loads(_node_state.decode("utf-8"))
        assert _node_state_json["role"] == _Crawler_Role_Value, _assertion

        _task_json = json.loads(_task.decode("utf-8"))
        assert _task_json["running_content"] == [], _assertion
        assert _task_json["running_status"] == _Task_Running_State, _assertion

        _heartbeat_json = json.loads(_heartbeat.decode("utf-8"))
        assert _heartbeat_json["heart_rhythm_time"] is not None, _assertion

    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    def test_is_ready_for_election_with_positive_timeout(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        uit_object.register_metadata()
        _func_start_time = time.time()
        _result = uit_object.is_ready_for_election(timeout=_Waiting_Time)
        _func_end__time = time.time()

        # Verify the values
        assert _result is False, "It should be *False* because the property *current_crawler* size is still only 1."
        assert _Waiting_Time <= (_func_end__time - _func_start_time) < (_Waiting_Time + 1), \
            f"The function running time should be done about {_Waiting_Time} - {_Waiting_Time + 1} seconds."

    @ZK.reset_testing_env(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    @ZK.remove_node_finally(path=[ZKNode.GroupState, ZKNode.NodeState, ZKNode.Task, ZKNode.Heartbeat])
    def test_elect(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        uit_object.register_metadata()
        _election_result = uit_object.elect()

        # Verify the values
        assert _election_result is ElectionResult.Winner, "It should be *ElectionResult.Winner* after the election with only one member."

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

    def _run_multi_processes(self, target_function, index_sep_char: str) -> List[mp.Process]:
        _processes = []
        for i in range(1, _Total_Crawler_Value + 1):
            _crawler_process = mp.Process(target=target_function, args=(f"sc-crawler{index_sep_char}{i}",))
            _crawler_process.daemon = True
            _processes.append(_crawler_process)

        for _process in _processes:
            _process.start()

        for _process in _processes:
            _process.join()

        return _processes

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

    def test_wait_for_standby(self, uit_object: ZookeeperCrawler):
        # Prepare the meta data for testing this scenario
        # Set a *GroupState* with only 2 crawlers and standby ID is '1'

        _all_paths: list = []

        def _initial_group_state() -> None:
            _state = Initial.group_state(
                crawler_name="sc-crawler_0",
                total_crawler=3,
                total_runner=2,
                total_backup=1,
                standby_id="1",
                current_crawler=["sc-crawler_0", "sc-crawler_1", "sc-crawler_2"],
                current_runner=["sc-crawler_0", "sc-crawler_2"],
                current_backup=["sc-crawler_1"]
            )
            if self._exist_node(path=uit_object.group_state_zookeeper_path) is None:
                _state_data_str = json.dumps(_state.to_readable_object())
                self._create_node(path=uit_object.group_state_zookeeper_path, value=bytes(_state_data_str, "utf-8"), include_data=True)

        def _initial_node_state(_node_path: str) -> None:
            _all_paths.append(_node_path)
            _node_state = Initial.node_state(group=uit_object._crawler_group, role=CrawlerStateRole.Runner)
            if self._exist_node(path=_node_path) is None:
                _node_state_data_str = json.dumps(_node_state.to_readable_object())
                self._create_node(path=_node_path, value=bytes(_node_state_data_str, "utf-8"), include_data=True)

        def _initial_task(_task_path: str) -> None:
            _all_paths.append(_task_path)
            _task = Initial.task(running_state=TaskResult.Processing)
            if self._exist_node(path=_task_path) is None:
                _task_data_str = json.dumps(_task.to_readable_object())
                self._create_node(path=_task_path, value=bytes(_task_data_str, "utf-8"), include_data=True)

        def _initial_heartbeat(_heartbeat_path: str) -> None:
            _all_paths.append(_heartbeat_path)
            _heartbeat = Initial.heartbeat()
            if self._exist_node(path=_heartbeat_path) is None:
                _heartbeat_data_str = json.dumps(_heartbeat.to_readable_object())
                self._create_node(path=_heartbeat_path, value=bytes(_heartbeat_data_str, "utf-8"), include_data=True)

        _initial_group_state()
        _all_paths.append(uit_object.group_state_zookeeper_path)
        _all_paths.append(uit_object.node_state_zookeeper_path)
        _all_paths.append(uit_object.task_zookeeper_path)
        _all_paths.append(uit_object.heartbeat_zookeeper_path)

        # Set a *NodeState* of sc-crawler_0
        _initial_node_state(uit_object.node_state_zookeeper_path.replace("1", "0"))

        # Set a *NodeState* of sc-crawler_1
        _initial_node_state(uit_object.node_state_zookeeper_path)

        # Set a *NodeState* of sc-crawler_2
        _initial_node_state(uit_object.node_state_zookeeper_path.replace("1", "2"))

        # Set a *Task* of sc-crawler_0
        _initial_task(uit_object.task_zookeeper_path.replace("1", "0"))

        # Set a *Task* of sc-crawler_1
        _initial_task(uit_object.task_zookeeper_path)

        # Set a *Task* of sc-crawler_2
        _initial_task(uit_object.task_zookeeper_path.replace("1", "2"))

        # Set a *Heartbeat* of sc-crawler_0 is dead
        _initial_heartbeat(uit_object.heartbeat_zookeeper_path.replace("1", "0"))

        # Set a *Heartbeat* of sc-crawler_2 is dead
        _initial_heartbeat(uit_object.heartbeat_zookeeper_path.replace("1", "2"))

        try:
            if uit_object.is_ready_for_run(timeout=5):
                uit_object.wait_and_standby()
            else:
                assert False, "It should be ready to run. Please check the detail implementation or other settings in testing has problem or not."

            # Verify the result should be correct as expected
            # Verify the *GroupState* info
            _data, _state = self._get_value_from_node(path=uit_object.group_state_zookeeper_path)
            _json_data = json.loads(str(_data.decode("utf-8")))
            print(f"[DEBUG in testing] _json_data: {_json_data}")
            assert _json_data["standby_id"] == "2", ""

            assert len(_json_data["current_crawler"]) == 2, ""
            assert uit_object._crawler_name in _json_data["current_crawler"], ""
            assert len(_json_data["current_runner"]) == 2, ""
            assert uit_object._crawler_name in _json_data["current_runner"], ""
            assert len(_json_data["current_backup"]) == 0, ""

            assert len(_json_data["fail_crawler"]) == 1, ""
            assert _json_data["fail_crawler"][0] != "sc-crawler_1", ""
            assert len(_json_data["fail_runner"]) == 1, ""
            assert _json_data["fail_runner"][0] != "sc-crawler_1", ""
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
            self._delete_zk_nodes(_all_paths)

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
            _zk_crawler._MetaData_Util.set_metadata_to_zookeeper(path=_zk_crawler.group_state_zookeeper_path, metadata=_state)

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


class MultiCrawlerTestSuite(ZK):

    __Processes: List[mp.Process] = []

    @staticmethod
    def _clean_environment(function):
        def _(self):
            # Initial Zookeeper session
            self._PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
            self._PyTest_ZK_Client.start()

            # Reset Zookeeper nodes first
            self._reset_all_metadata(size=_Total_Crawler_Value)

            try:
                # Run the test item
                function(self)
            finally:
                # Kill all processes
                for _process in self.__Processes:
                    _process.terminate()
                # Reset Zookeeper nodes fianlly
                self._reset_all_metadata(size=_Total_Crawler_Value)
        return _

    def _reset_all_metadata(self, size: int) -> None:
        _all_paths = _ZKNodePathUtils.all(size)
        self._delete_zk_nodes(_all_paths)


class TestZookeeperCrawlerFeatureWithMultipleCrawlers(MultiCrawlerTestSuite):

    @MultiCrawlerTestSuite._clean_environment
    def test_register_metadata_and_elect_with_many_crawler_instances(self):
        _running_flag: Dict[str, bool] = _Manager.dict()
        _is_ready_flag: Dict[str, bool] = _Manager.dict()
        _election_results: Dict[str, ElectionResult] = _Manager.dict()

        _index_sep_char: str = "_"

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

                # Run target methods
                _zk_crawler.ensure_register = True
                _zk_crawler.ensure_timeout = 10
                _zk_crawler.ensure_wait = 0.5
                _zk_crawler.register_metadata()

                # Verify the running result
                _zk_crawler_ready = _zk_crawler.is_ready_for_election(timeout=10)
                _is_ready_flag[_name] = _zk_crawler_ready

                # Try to run election
                _elect_result = _zk_crawler.elect()
                _election_results[_name] = _elect_result
            except:
                _running_flag[_name] = False
                raise
            else:
                _running_flag[_name] = True

        # Run the target methods by multi-threads
        self._run_multi_processes(target_function=_run_and_test, index_sep_char=_index_sep_char)
        self._check_running_status(_running_flag)

        # Verify the running result by the value from Zookeeper
        _data, _state = self._PyTest_ZK_Client.get(path=_Testing_Value.group_state_zookeeper_path)
        _json_data = json.loads(_data.decode("utf-8"))
        print(f"[DEBUG] _state_path: {_Testing_Value.group_state_zookeeper_path}, _json_data: {_json_data}")
        self._check_current_crawler(_json_data, _running_flag)
        self._check_is_ready_flags(_is_ready_flag)
        self._check_election_results(_election_results, _index_sep_char)

    @MultiCrawlerTestSuite._clean_environment
    def test_many_crawler_instances_with_initial(self):
        _running_flag: Dict[str, bool] = _Manager.dict()
        _role_results: Dict[str, CrawlerStateRole] = _Manager.dict()

        _index_sep_char: str = "_"

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

                # Get the instance role of the crawler cluster
                _role_results[_name] = _zk_crawler.role
            except:
                _running_flag[_name] = False
                raise
            else:
                _running_flag[_name] = True

        # Run the target methods by multi-threads
        self._run_multi_processes(target_function=_run_and_test, index_sep_char=_index_sep_char)

        # Verify the running state
        self._check_running_status(_running_flag)

        # Verify the heartbeat info
        _heartbeat_paths = _ZKNodePathUtils.all_heartbeat(size=_Runner_Crawler_Value + _Backup_Crawler_Value)
        self._check_heartbeat_info(list(_heartbeat_paths))

        # Verify the running result by the value from Zookeeper
        _data, _state = self._PyTest_ZK_Client.get(path=_Testing_Value.group_state_zookeeper_path)
        _json_data = json.loads(_data.decode("utf-8"))
        print(f"[DEBUG] _state_path: {_Testing_Value.group_state_zookeeper_path}, _json_data: {_json_data}")
        self._check_current_crawler(_json_data, _running_flag)
        self._check_current_runner(_json_data, _index_sep_char)
        self._check_current_backup_and_standby_id(_json_data, _index_sep_char)
        self._check_role(_role_results, _index_sep_char)

    @MultiCrawlerTestSuite._clean_environment
    def test_wait_for_task(self):
        _empty_task = Empty.task()
        if self._exist_node(path=_Testing_Value.task_zookeeper_path) is None:
            _task_data_str = json.dumps(_empty_task.to_readable_object())
            self._create_node(path=_Testing_Value.task_zookeeper_path, value=bytes(_task_data_str, "utf-8"), include_data=True)

        # Instantiate ZookeeperCrawler
        _zk_crawler = ZookeeperCrawler(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            initial=False,
            zk_hosts=Zookeeper_Hosts
        )

        def _assign_task() -> None:
            time.sleep(5)
            _task = _zk_crawler._MetaData_Util.get_metadata_from_zookeeper(path=_Testing_Value.task_zookeeper_path, as_obj=Task)
            _updated_task = Update.task(_task, running_content=_Task_Running_Content_Value)
            _updated_task_str = json.dumps(_updated_task.to_readable_object())
            self._set_value_to_node(path=_Testing_Value.task_zookeeper_path, value=bytes(_updated_task_str, "utf-8"))

        def _wait_for_task() -> None:
            try:
                _zk_crawler.wait_for_task()
            except NotImplementedError:
                assert True, "It works finely."
            else:
                assert False, "It should raise an error about *NotImplementedError* of registering SmoothCrawler components."

            try:
                _zk_crawler.register_factory(
                    http_req_sender=RequestsHTTPRequest(),
                    http_resp_parser=RequestsHTTPResponseParser(),
                    data_process=ExampleWebDataHandler()
                )
                _zk_crawler.wait_for_task()
            except Exception:
                assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
            else:
                assert True, "It works finely."

        self._run_2_diff_processes(func1_ps=(_assign_task, (), False), func2_ps=(_wait_for_task, (), True))

        time.sleep(3)

        # Verify the running result
        _task_data, state = self._get_value_from_node(path=_zk_crawler.task_zookeeper_path)
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

    def _run_multi_processes(self, target_function, index_sep_char: str = "_") -> None:
        self.__Processes = []
        for i in range(1, _Total_Crawler_Value + 1):
            _crawler_process = mp.Process(target=target_function, args=(f"sc-crawler{index_sep_char}{i}",))
            _crawler_process.daemon = True
            self.__Processes.append(_crawler_process)

        for _process in self.__Processes:
            _process.start()

        for _process in self.__Processes:
            _process.join()

    def _run_2_diff_processes(self, func1_ps: tuple, func2_ps: tuple):
        func1, func1_args, func1_daemon = func1_ps
        func2, func2_args, func2_daemon = func2_ps

        self.__Processes = []
        _func1_process = threading.Thread(target=func1, args=(func1_args or ()))
        _func2_process = threading.Thread(target=func2, args=(func2_args or ()))
        _func1_process.daemon = func1_daemon
        _func2_process.daemon = func2_daemon

        _func1_process.start()
        _func2_process.start()

        if func1_daemon is False:
            _func1_process.join()
        if func2_daemon is False:
            _func2_process.join()

    @classmethod
    def _check_running_status(cls, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread gets any exception in running."

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

    def _check_current_runner(self, json_data, index_sep_char: str) -> None:
        _current_runner = json_data["current_runner"]
        assert len(_current_runner) == _Runner_Crawler_Value, f"The size of attribute *current_runner* should be same as {_Runner_Crawler_Value}."
        _runer_checksum_list = list(
            map(lambda _crawler: int(_crawler.split(index_sep_char)[-1]) <= _Runner_Crawler_Value,
                _current_runner))
        assert False not in _runer_checksum_list, f"The index of all crawler name should be <= {_Runner_Crawler_Value} (the count of runner)."

    def _check_election_results(self, election_results: Dict[str, ElectionResult], index_sep_char: str) -> None:
        assert len(election_results.keys()) == _Total_Crawler_Value, \
            f"The size of *elect* feature checksum should be {_Total_Crawler_Value}."
        for _crawler_name, _election_result in election_results.items():
            _crawler_index = int(_crawler_name.split(index_sep_char)[-1])
            if _crawler_index <= _Runner_Crawler_Value:
                assert _election_result is ElectionResult.Winner, f"The election result of '{_crawler_name}' should be *ElectionResult.Winner*."
            else:
                assert _election_result is ElectionResult.Loser, f"The election result of '{_crawler_name}' should be *ElectionResult.Loser*."

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


class TestZookeeperCrawlerRunUnderDiffScenarios(MultiCrawlerTestSuite):

    __All_Node_State_Paths: List[str] = []
    __All_Task_State_Paths: List[str] = []
    __All_Heartbeat_State_Paths: List[str] = []

    @MultiCrawlerTestSuite._clean_environment
    def test_run_with_2_runner_and_1_backup(self):
        # Run the target methods by multi-processes
        _running_flag, _role_results = self._run_multiple_crawler_instances(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            delay_assign_task=3
        )

        time.sleep(3)    # Wait for thread 2 dead and thread 3 activate itself to be runner.

        self._verify_exception()
        self._check_running_status(_running_flag)

        # Verify running result from meta-data
        self._verify_results(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            fail_runner=_Fail_Runner_Crawler_Value,
            expected_role={
                "1": CrawlerStateRole.Runner,
                "2": CrawlerStateRole.Dead_Runner,
                "3": CrawlerStateRole.Runner
            },
            expected_task_result={
                "1": "available",
                "2": "available",
                "3": "available"
            }
        )

    @MultiCrawlerTestSuite._clean_environment
    def test_run_with_multiple_backup(self):
        _Multiple_Backup_Scenarios_Runner_Crawler: int = 2
        _Multiple_Backup_Scenarios_Backup_Crawler: int = 2

        # Run the target methods by multi-processes
        _running_flag, _role_results = self._run_multiple_crawler_instances(
            runner=_Multiple_Backup_Scenarios_Runner_Crawler,
            backup=_Multiple_Backup_Scenarios_Backup_Crawler,
            delay=True
        )

        time.sleep(15)    # Wait for thread 2 dead and thread 3 activate itself to be runner.

        self._verify_exception()
        self._check_running_status(_running_flag)

        # Verify running result from meta-data
        self._verify_results(
            runner=_Multiple_Backup_Scenarios_Runner_Crawler,
            backup=_Multiple_Backup_Scenarios_Backup_Crawler,
            fail_runner=_Fail_Runner_Crawler_Value,
            expected_role={
                "1": CrawlerStateRole.Runner,
                "2": CrawlerStateRole.Dead_Runner,
                "3": CrawlerStateRole.Runner,
                "4": CrawlerStateRole.Backup_Runner
            },
            expected_task_result={
                "1": "available",
                "2": "available",
                "3": "available",
                "4": "backup"
            }
        )

    def test_run_with_multiple_fail_runners(self):
        pass

    def _run_multiple_crawler_instances(self, runner: int, backup: int, delay: bool = False, delay_assign_task: int = None):
        _running_flag: Dict[str, bool] = _Manager.dict()
        _role_results: Dict[str, CrawlerStateRole] = _Manager.dict()

        def _assign_task() -> None:
            _task_paths = _ZKNodePathUtils.all_task(runner)
            for _task_path in _task_paths:
                if delay is True and "2" in _task_path:
                    _task_running_content = _One_Running_Content
                    _task_running_content["url"] = _One_Running_Content["url"] + "?sleep=5"
                    _task_running_contents = [_task_running_content]
                else:
                    _task_running_contents = _Task_Running_Content_Value
                _task = Initial.task(running_content=_task_running_contents)
                if self._exist_node(_task_path) is None:
                    self._create_node(path=_task_path, value=bytes(json.dumps(_task.to_readable_object()), "utf-8"), include_data=True)
                else:
                    self._set_value_to_node(path=_task_path, value=bytes(json.dumps(_task.to_readable_object()), "utf-8"))

        def _run_and_test(_name: str) -> None:
            _zk_crawler = None
            try:
                # Instantiate ZookeeperCrawler
                if "2" in _name:
                    _zk_crawler = ZookeeperCrawler(
                        runner=runner,
                        backup=backup,
                        name=_name,
                        initial=False,
                        ensure_initial=True,
                        ensure_timeout=20,
                        ensure_wait=1,
                        zk_hosts=Zookeeper_Hosts
                    )
                    _zk_crawler.stop_update_heartbeat()
                    _zk_crawler.initial()
                else:
                    if delay is True and "3" in _name:
                        time.sleep(3)
                    _zk_crawler = ZookeeperCrawler(
                        runner=runner,
                        backup=backup,
                        name=_name,
                        initial=True,
                        ensure_initial=True,
                        ensure_timeout=20,
                        ensure_wait=1,
                        zk_hosts=Zookeeper_Hosts
                    )

                # Get the instance role of the crawler cluster
                _role_results[_name] = _zk_crawler.role

                _zk_crawler.register_factory(
                    http_req_sender=RequestsHTTPRequest(),
                    http_resp_parser=RequestsHTTPResponseParser(),
                    data_process=ExampleWebDataHandler()
                )
                _zk_crawler.run()
            except Exception as e:
                _running_flag[_name] = False
                global _Global_Exception_Record
                _Global_Exception_Record = e
                print(f"[DEBUG - {_name}] e: {e}")
                raise e
            else:
                _running_flag[_name] = True

        self.__Processes = []
        # Run the target methods by multi-processes
        for i in range(1, (runner + backup + 1)):
            _crawler_process = mp.Process(target=_run_and_test, args=(f"sc-crawler_{i}",))
            _crawler_process.daemon = True
            self.__Processes.append(_crawler_process)

        for _process in self.__Processes:
            _process.start()

        if delay_assign_task is not None:
            time.sleep(delay_assign_task)
        _assign_task()

        return _running_flag, _role_results

    def _verify_exception(self):
        global _Global_Exception_Record
        print(f"[DEBUG in testing] _Global_Exception_Record: {_Global_Exception_Record}")
        if _Global_Exception_Record is not None:
            assert False, traceback.print_exception(_Global_Exception_Record)

    def _verify_results(self, runner: int, backup: int, fail_runner: int, expected_role, expected_task_result):
        # Verify the group info
        self._verify_group_info(runner=runner, backup=backup, fail_runner=fail_runner)
        # Verify the state info
        self._verify_state_role(runner=runner, backup=backup, expected_role=expected_role)
        # Verify the task info
        self._verify_task_detail(runner=runner, backup=backup, expected_task_result=expected_task_result)


    def _verify_group_info(self, runner: int, backup: int, fail_runner: int):
        _group_data, _state = self._PyTest_ZK_Client.get(path=_Testing_Value.group_state_zookeeper_path)
        assert len(_group_data) != 0, "The data content of meta data *GroupState* should NOT be empty."
        _group_json_data = json.loads(_group_data.decode("utf-8"))
        print(f"[DEBUG in testing] _group_state_path.value: {_Testing_Value.group_state_zookeeper_path}, _group_json_data: {_group_json_data} - {datetime.now()}")

        assert _group_json_data["total_crawler"] == runner + backup, ""
        assert _group_json_data["total_runner"] == runner, ""
        assert _group_json_data["total_backup"] == backup - fail_runner, ""

        assert len(_group_json_data["current_crawler"]) == runner + backup - fail_runner, ""
        assert False not in ["2" not in _crawler for _crawler in _group_json_data["current_crawler"]], ""
        assert len(_group_json_data["current_runner"]) == runner, ""
        assert False not in ["2" not in _crawler for _crawler in _group_json_data["current_runner"]], ""
        assert len(_group_json_data["current_backup"]) == backup - fail_runner, ""

        assert _group_json_data["standby_id"] == "4", ""

        assert len(_group_json_data["fail_crawler"]) == fail_runner, ""
        assert False not in ["2" in _crawler for _crawler in _group_json_data["fail_crawler"]], ""
        assert len(_group_json_data["fail_runner"]) == fail_runner, ""
        assert False not in ["2" in _crawler for _crawler in _group_json_data["fail_runner"]], ""
        assert len(_group_json_data["fail_backup"]) == 0, ""

    def _verify_state_role(self, runner: int, backup: int, expected_role: dict):
        _state_paths = _ZKNodePathUtils.all_node_state(runner + backup)
        for _state_path in list(_state_paths):
            _data, _state = self._PyTest_ZK_Client.get(path=_state_path)
            _json_data = json.loads(_data.decode("utf-8"))
            print(f"[DEBUG in testing] _state_path: {_state_path}, _json_data: {_json_data} - {datetime.now()}")
            _chksum = re.search(r"[0-9]{1,3}", _state_path)
            if _chksum is not None:
                assert _json_data["role"] == expected_role[_chksum.group(0)].value, ""
            else:
                assert False, ""

    def _verify_task_detail(self, runner: int, backup: int, expected_task_result: dict):
        _task_paths = _ZKNodePathUtils.all_task(runner + backup)
        for _task_path in list(_task_paths):
            _data, _state = self._PyTest_ZK_Client.get(path=_task_path)
            _json_data = json.loads(_data.decode("utf-8"))
            print(f"[DEBUG in testing] _task_path: {_task_path}, _json_data: {_json_data}")
            assert _json_data is not None, ""
            _chksum = re.search(r"[0-9]{1,3}", _task_path)
            if _chksum is not None:
                _chk_detail_assert = getattr(self, f"_chk_{expected_task_result[_chksum.group(0)]}_task_detail")
                _chk_detail_assert(_json_data)
            else:
                assert False, ""

    @classmethod
    def _check_running_status(cls, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread gets any exception in running."

    @classmethod
    def _chk_available_task_detail(cls, _one_detail: dict) -> None:
        assert len(_one_detail["running_content"]) == 0, ""
        assert _one_detail["in_progressing_id"] == "-1", ""
        assert _one_detail["running_result"] == {"success_count": 1, "fail_count": 0}, ""
        assert _one_detail["running_status"] == TaskResult.Done.value, ""
        assert _one_detail["result_detail"][0] == {
            "task_id": _Task_Running_Content_Value[0]["task_id"],
            "state": TaskResult.Done.value,
            "status_code": 200,
            "response": "Example Domain",
            "error_msg": None
        }, "The detail should be completely same as above."

    @classmethod
    def _chk_backup_task_detail(cls, _one_detail: dict) -> None:
        assert len(_one_detail["running_content"]) == 0, ""
        assert _one_detail["in_progressing_id"] == "-1", ""
        assert _one_detail["running_result"] == {"success_count": 0, "fail_count": 0}, ""
        assert _one_detail["running_status"] == TaskResult.Nothing.value, ""
        assert len(_one_detail["result_detail"]) == 0, "It should NOT have any running result because it is backup role."
