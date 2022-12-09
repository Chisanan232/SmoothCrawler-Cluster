from smoothcrawler_cluster.model import Initial, Update, CrawlerStateRole, TaskResult, GroupState, NodeState
from smoothcrawler_cluster.election import ElectionResult
from smoothcrawler_cluster.crawler import ZookeeperCrawler
from kazoo.client import KazooClient
from typing import List, Dict
import multiprocessing as mp
import traceback
import pytest
import json
import time

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
from .._verify import VerifyMetaData
from ._test_utils._instance_value import _TestValue, _ZKNodePathUtils
from ._test_utils._zk_testsuite import ZK, ZKNode, ZKTestSpec
from ._test_utils._multirunner import run_multi_processes, run_2_diff_workers


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


class MultiCrawlerTestSuite(ZK):

    _Processes: List[mp.Process] = []

    _Verify = VerifyMetaData()

    @staticmethod
    def _clean_environment(function):
        def _(self):
            # Initial Zookeeper session
            self._PyTest_ZK_Client = KazooClient(hosts=Zookeeper_Hosts)
            self._PyTest_ZK_Client.start()

            self._Verify.initial_zk_session(self._PyTest_ZK_Client)

            # Reset Zookeeper nodes first
            self._reset_all_metadata(size=_Total_Crawler_Value)

            # Reset workers collection
            self._Processes.clear()

            try:
                # Run the test item
                function(self)
            finally:
                # Kill all processes
                for _process in self._Processes:
                    if type(_process) is mp.Process:
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
        self._Processes = run_multi_processes(processes_num=_Total_Crawler_Value, target_function=_run_and_test, index_sep_char=_index_sep_char)
        self._check_running_status(_running_flag)

        # Verify the running result by the value from Zookeeper
        _data, _state = self._PyTest_ZK_Client.get(path=_Testing_Value.group_state_zookeeper_path)
        _json_data = json.loads(_data.decode("utf-8"))
        print(f"[DEBUG] _state_path: {_Testing_Value.group_state_zookeeper_path}, _json_data: {_json_data}")
        self._Verify.group_state_current_section(runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, verify_runner=False, verify_backup=False)
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
        self._Processes = run_multi_processes(processes_num=_Total_Crawler_Value, target_function=_run_and_test, index_sep_char=_index_sep_char)

        # Verify the running state
        self._check_running_status(_running_flag)

        # Verify the heartbeat info
        self._Verify.all_heartbeat_info(runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value)

        # Verify the running result by the value from Zookeeper
        self._Verify.group_state_info(runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, standby_id="3")
        self._check_role(_role_results, _index_sep_char)

    @MultiCrawlerTestSuite._clean_environment
    def test_wait_for_task(self):

        def _assign_task() -> None:
            time.sleep(2)
            _task = Initial.task()
            _updated_task = Update.task(_task, running_content=_Task_Running_Content_Value)
            _updated_task_str = json.dumps(_updated_task.to_readable_object())
            self._set_value_to_node(path=_Testing_Value.task_zookeeper_path, value=bytes(_updated_task_str, "utf-8"))

        def _wait_for_task() -> None:
            # Instantiate ZookeeperCrawler
            _zk_crawler = ZookeeperCrawler(
                name=_Testing_Value.name,
                runner=_Runner_Crawler_Value,
                backup=_Backup_Crawler_Value,
                initial=False,
                zk_hosts=Zookeeper_Hosts
            )
            _zk_crawler.register_task()

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

        run_2_diff_workers(func1_ps=(_assign_task, (), False), func2_ps=(_wait_for_task, (), True), worker="thread")

        time.sleep(3)

        # Verify the running result
        _task_data, state = self._get_value_from_node(path=_Testing_Value.task_zookeeper_path)
        print(f"[DEBUG in testing] _task_data: {_task_data}")
        self._Verify.one_task_info(
            _task_data,
            in_progressing_id="-1",
            running_result={"success_count": 1, "fail_count": 0},
            running_status=TaskResult.Done.value,
            result_detail_len=1
        )
        self._Verify.one_task_result_detail(
            _task_data,
            task_path=_Testing_Value.task_zookeeper_path,
            expected_task_result={"1": "available"}
        )

    @MultiCrawlerTestSuite._clean_environment
    def test_wait_for_standby(self):
        # Instantiate ZookeeperCrawler
        _zk_crawler = ZookeeperCrawler(
            runner=1,
            backup=1,
            initial=False,
            zk_hosts=Zookeeper_Hosts
        )

        def _initial_group_state() -> None:
            _state = Initial.group_state(
                crawler_name="sc-crawler_0",
                total_crawler=2,
                total_runner=1,
                total_backup=1,
                standby_id="1",
                current_crawler=["sc-crawler_0", "sc-crawler_1"],
                current_runner=["sc-crawler_0"],
                current_backup=["sc-crawler_1"]
            )
            if self._exist_node(path=_Testing_Value.group_state_zookeeper_path) is None:
                _state_data_str = json.dumps(_state.to_readable_object())
                self._create_node(path=_Testing_Value.group_state_zookeeper_path, value=bytes(_state_data_str, "utf-8"), include_data=True)

        def _initial_node_state(_node_path: str) -> None:
            _node_state = Initial.node_state(group=_zk_crawler.group, role=CrawlerStateRole.Runner)
            if self._exist_node(path=_node_path) is None:
                _node_state_data_str = json.dumps(_node_state.to_readable_object())
                self._create_node(path=_node_path, value=bytes(_node_state_data_str, "utf-8"), include_data=True)

        def _initial_task(_task_path: str) -> None:
            _task = Initial.task(running_state=TaskResult.Processing)
            if self._exist_node(path=_task_path) is None:
                _task_data_str = json.dumps(_task.to_readable_object())
                self._create_node(path=_task_path, value=bytes(_task_data_str, "utf-8"), include_data=True)

        def _initial_heartbeat(_heartbeat_path: str) -> None:
            _heartbeat = Initial.heartbeat()
            if self._exist_node(path=_heartbeat_path) is None:
                _heartbeat_data_str = json.dumps(_heartbeat.to_readable_object())
                self._create_node(path=_heartbeat_path, value=bytes(_heartbeat_data_str, "utf-8"), include_data=True)

        _initial_group_state()

        # Set a *NodeState* of all crawler instances
        _all_node_state_paths = _ZKNodePathUtils.all_node_state(size=2, start_index=0)
        for _node_state_path in _all_node_state_paths:
            _initial_node_state(_node_state_path)

        # Set a *Task* of all crawler instances
        _all_task_paths = _ZKNodePathUtils.all_task(size=2, start_index=0)
        for _task_path in _all_task_paths:
            _initial_task(_task_path)

        # Set a *Task* of all crawler instances
        _all_heartbeat_paths = _ZKNodePathUtils.all_heartbeat(size=2, start_index=0)
        for _heartbeat_path in _all_heartbeat_paths:
            _initial_heartbeat(_heartbeat_path)

        if _zk_crawler.is_ready_for_run(timeout=5):
            _zk_crawler.wait_and_standby()
        else:
            assert False, "It should be ready to run. Please check the detail implementation or other settings in testing has problem or not."

        # Verify the result should be correct as expected
        # Verify the *GroupState* info
        self._Verify.group_state_info(runner=1, backup=1, fail_runner=1, standby_id="2")

        # Verify the *NodeState* info
        self._Verify.all_node_state_role(
            runner=1, backup=1,
            expected_role={"0": CrawlerStateRole.Dead_Runner, "1": CrawlerStateRole.Runner},
            expected_group={"0": _zk_crawler._crawler_group, "1": _zk_crawler._crawler_group},
            start_index=0
        )

    @MultiCrawlerTestSuite._clean_environment
    def test_wait_for_to_be_standby(self):
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

        _result = _Manager.Value("_result", None)
        _start = _Manager.Value("_start", None)
        _end = _Manager.Value("_end", None)

        def _update_state_standby_id():
            time.sleep(5)
            _state.standby_id = "2"
            _zk_crawler._MetaData_Util.set_metadata_to_zookeeper(path=_zk_crawler.group_state_zookeeper_path, metadata=_state)

        def _run_target_test_func():
            try:
                nonlocal _result, _start, _end
                _start.value = time.time()
                # # Run target function
                _result.value = _zk_crawler.wait_for_to_be_standby()
                _end.value = time.time()
            finally:
                if self._exist_node(path=_zk_crawler.group_state_zookeeper_path):
                    self._delete_node(path=_zk_crawler.group_state_zookeeper_path)

        run_2_diff_workers(func1_ps=(_update_state_standby_id, (), False), func2_ps=(_run_target_test_func, (), False), worker="thread")

        # # Verify the result
        assert _result.value is True, "It should be True after it detect the stand ID to be '2'."
        assert 5 < int(_end.value - _start.value) <= 6, "It should NOT run more than 6 seconds."

    @classmethod
    def _check_running_status(cls, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread gets any exception in running."

    def _check_is_ready_flags(self, is_ready_flag: Dict[str, bool]) -> None:
        assert len(is_ready_flag.keys()) == _Total_Crawler_Value, \
            f"The size of *is_ready* feature checksum should be {_Total_Crawler_Value}."
        assert False not in is_ready_flag, \
            "It should NOT exist any checksum element is False (it means crawler doesn't ready for running election)."

    def _check_election_results(self, election_results: Dict[str, ElectionResult], index_sep_char: str) -> None:
        assert len(election_results.keys()) == _Total_Crawler_Value, f"The size of *elect* feature checksum should be {_Total_Crawler_Value}."
        for _crawler_name, _election_result in election_results.items():
            _crawler_index = int(_crawler_name.split(index_sep_char)[-1])
            if _crawler_index <= _Runner_Crawler_Value:
                assert _election_result is ElectionResult.Winner, f"The election result of '{_crawler_name}' should be *ElectionResult.Winner*."
            else:
                assert _election_result is ElectionResult.Loser, f"The election result of '{_crawler_name}' should be *ElectionResult.Loser*."

    def _check_role(self, role_results: Dict[str, CrawlerStateRole], index_sep_char: str) -> None:
        assert len(role_results.keys()) == _Total_Crawler_Value, f"The size of *role* attribute checksum should be {_Total_Crawler_Value}."
        for _crawler_name, _role in role_results.items():
            _crawler_index = int(_crawler_name.split(index_sep_char)[-1])
            if _crawler_index <= _Runner_Crawler_Value:
                assert _role is CrawlerStateRole.Runner, f"The role of this crawler instance '{_crawler_name}' should be '{CrawlerStateRole.Runner}'."
            else:
                assert _role is CrawlerStateRole.Backup_Runner, f"The role of this crawler instance '{_crawler_name}' should be '{CrawlerStateRole.Backup_Runner}'."


class TestZookeeperCrawlerRunUnderDiffScenarios(MultiCrawlerTestSuite):

    @MultiCrawlerTestSuite._clean_environment
    def test_run_with_2_runner_and_1_backup(self):
        # Run the target methods by multi-processes
        _running_flag, _role_results = self._run_multiple_crawler_instances(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            delay_assign_task=5
        )

        time.sleep(8)    # Wait for thread 2 dead and thread 3 activate itself to be runner.

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
            expected_group={
                "1": "sc-crawler-cluster",
                "2": "sc-crawler-cluster",
                "3": "sc-crawler-cluster"
            },
            expected_task_result={
                "1": "available",
                "2": "available",
                "3": "nothing"
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
            delay=True,
            delay_assign_task=5
        )

        time.sleep(20)    # Wait for thread 2 dead and thread 3 activate itself to be runner.

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
            expected_group={
                "1": "sc-crawler-cluster",
                "2": "sc-crawler-cluster",
                "3": "sc-crawler-cluster",
                "4": "sc-crawler-cluster"
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

        self._Processes = []
        # Run the target methods by multi-processes
        for i in range(1, (runner + backup + 1)):
            _crawler_process = mp.Process(target=_run_and_test, args=(f"sc-crawler_{i}",))
            _crawler_process.daemon = True
            self._Processes.append(_crawler_process)

        for _process in self._Processes:
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

    def _verify_results(self, runner: int, backup: int, fail_runner: int, expected_role: dict, expected_group: dict, expected_task_result: dict):
        # Verify the group info
        self._Verify.group_state_info(runner=runner, backup=backup, fail_runner=fail_runner)
        # Verify the state info
        self._Verify.all_node_state_role(runner=runner, backup=backup, expected_role=expected_role, expected_group=expected_group)
        # Verify the task info
        self._Verify.all_task_detail(runner=runner, backup=backup, expected_task_result=expected_task_result)

    @classmethod
    def _check_running_status(cls, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread gets any exception in running."
