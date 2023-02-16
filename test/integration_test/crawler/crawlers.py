import json
import multiprocessing as mp
import time
import traceback
from datetime import datetime
from typing import Dict, List, Optional, Union

import pytest
from kazoo.client import KazooClient

from smoothcrawler_cluster.crawler.crawlers import ZookeeperCrawler
from smoothcrawler_cluster.election import ElectionResult
from smoothcrawler_cluster.model import (
    CrawlerRole,
    GroupState,
    Initial,
    NodeState,
    TaskState,
    Update,
)

from ..._config import Zookeeper_Hosts
from ..._sample_components._components import (
    ExampleWebDataHandler,
    RequestsHTTPRequest,
    RequestsHTTPResponseParser,
)
from ..._values import (
    _Backup_Crawler_Value,
    _Fail_Runner_Crawler_Value,
    _One_Running_Content,
    _One_Running_Content_As_Object,
    _Runner_Crawler_Value,
    _Task_Running_Content_Value,
    _Total_Crawler_Value,
    _Waiting_Time,
)
from ..._verify import VerifyMetaData
from .._test_utils._instance_value import _TestValue, _ZKNodePathUtils
from .._test_utils._multirunner import run_2_diff_workers, run_multi_processes
from .._test_utils._zk_testsuite import ZK, ZKNode, ZKTestSpec
from ._spec import MultiCrawlerTestSuite

_Manager = mp.Manager()
_Testing_Value: _TestValue = _TestValue()


class TestZookeeperCrawlerSingleInstance(ZKTestSpec):

    _verify_metadata = VerifyMetaData()

    @pytest.fixture(scope="function")
    def uit_object(self) -> ZookeeperCrawler:
        self._pytest_zk_client = KazooClient(hosts=Zookeeper_Hosts)
        self._pytest_zk_client.start()

        self._verify_metadata.initial_zk_session(client=self._pytest_zk_client)

        return ZookeeperCrawler(
            runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, initial=False, zk_hosts=Zookeeper_Hosts
        )

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE])
    @ZK.add_node_with_value_first(
        path_and_value={
            ZKNode.GROUP_STATE: _Testing_Value.group_state_data_str,
            ZKNode.NODE_STATE: _Testing_Value.node_state_data_str,
        }
    )
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE])
    def test__update_crawler_role(self, uit_object: ZookeeperCrawler):
        # Checking initial state
        group_state = uit_object._metadata_util.get_metadata_from_zookeeper(
            path=uit_object._zk_path.group_state, as_obj=GroupState
        )
        assert (
            len(group_state.current_crawler) == 0
        ), "At initial process, the length of current crawler list should be 0."
        assert (
            len(group_state.current_runner) == 0
        ), "At initial process, the length of current runner list should be 0."
        assert (
            len(group_state.current_backup) == 0
        ), "At initial process, the length of current backup list should be 0."

        node_state = uit_object._metadata_util.get_metadata_from_zookeeper(
            path=uit_object._zk_path.node_state, as_obj=NodeState
        )
        assert node_state.role == CrawlerRole.INITIAL.value, (
            "At initial process, the role of crawler instance should be *initial* (or value of "
            "*CrawlerStateRole.Initial*)."
        )

        # Checking initial state
        uit_object._update_crawler_role(CrawlerRole.RUNNER)

        # Verify the updated state
        updated_group_state = uit_object._metadata_util.get_metadata_from_zookeeper(
            path=uit_object._zk_path.group_state, as_obj=GroupState
        )
        assert (
            len(updated_group_state.current_runner) == 1
        ), "After update the *state* meta data, the length of current crawler list should be 1 because it's *runner*."
        assert (
            len(updated_group_state.current_backup) == 0
        ), "After update the *state* meta data, the length of current crawler list should be 0 because it's *runner*."

        updated_node_state = uit_object._metadata_util.get_metadata_from_zookeeper(
            path=uit_object._zk_path.node_state, as_obj=NodeState
        )
        assert (
            updated_node_state.role == CrawlerRole.RUNNER.value
        ), "After update the *state* meta data, its role should change to be *runner* (*CrawlerStateRole.Runner*)."

    @ZK.reset_testing_env(path=[ZKNode.HEARTBEAT])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.HEARTBEAT: _Testing_Value.heartbeat_data_str})
    @ZK.remove_node_finally(path=[ZKNode.HEARTBEAT])
    def test_stop_update_heartbeat(self, uit_object: ZookeeperCrawler):
        previous_heartbeat = self._verify_metadata._generate_heartbeat_data_opt()
        previous_heart_rhythm_time = previous_heartbeat.heart_rhythm_time
        update_time = previous_heartbeat.update_time

        uit_object.stop_update_heartbeat()

        time.sleep(float(update_time[:-1]) * 5)

        heartbeat = self._verify_metadata._generate_heartbeat_data_opt()
        heart_rhythm_time = heartbeat.heart_rhythm_time
        diff_time = datetime.strptime(heart_rhythm_time, heartbeat.time_format) - datetime.strptime(
            previous_heart_rhythm_time, heartbeat.time_format
        )
        assert diff_time.total_seconds() == 0, ""

    @ZK.reset_testing_env(path=ZKNode.GROUP_STATE)
    @ZK.add_node_with_value_first(path_and_value={ZKNode.GROUP_STATE: _Testing_Value.group_state_data_str})
    @ZK.remove_node_finally(path=ZKNode.GROUP_STATE)
    def test_is_ready_for_run_with_positive_timeout(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        is_ready = uit_object.is_ready_for_run(timeout=1)
        assert is_ready is False, "It should be False because it isn't ready for running by checking *GroupState*."

        # Update the meta-data *GroupState*
        group_state_data, state = self._get_value_from_node(path=_Testing_Value.group_state_zookeeper_path)
        group_state = json.loads(group_state_data.decode("utf-8"))
        group_state["current_crawler"].extend([uit_object.name, "test_runner", "test_backup"])
        group_state["current_runner"].extend([uit_object.name, "test_runner"])
        group_state["current_backup"].append("test_backup")
        self._set_value_to_node(
            path=_Testing_Value.group_state_zookeeper_path, value=bytes(json.dumps(group_state), "utf-8")
        )

        # Operate target method to test again
        is_ready = uit_object.is_ready_for_run(timeout=_Waiting_Time)

        # Verify
        assert is_ready is True, "It should be True because its conditions has been satisfied."

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    def test_is_ready_for_election_with_positive_timeout(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        uit_object.register_metadata()
        func_start_time = time.time()
        result = uit_object.is_ready_for_election(timeout=_Waiting_Time)
        func_end__time = time.time()

        # Verify the values
        assert result is False, "It should be *False* because the property *current_crawler* size is still only 1."
        assert (
            _Waiting_Time <= (func_end__time - func_start_time) < (_Waiting_Time + 1)
        ), f"The function running time should be done about {_Waiting_Time} - {_Waiting_Time + 1} seconds."

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    def test_elect(self, uit_object: ZookeeperCrawler):
        # Operate target method to test
        uit_object.register_metadata()
        election_result = uit_object.elect()

        # Verify the values
        assert (
            election_result is ElectionResult.WINNER
        ), "It should be *ElectionResult.Winner* after the election with only one member."

    def test__run_crawling_processing(self, uit_object: ZookeeperCrawler):
        try:
            uit_object._run_crawling_processing(content=_One_Running_Content_As_Object)
        except NotImplementedError as e:
            assert (
                str(e) == "You should implement the SmoothCrawler components and register them."
            ), "The error message should be the same."
        else:
            assert False, (
                "It should raise an error *NotImplementedError* about developer should implement and register "
                "SmoothCrawler components."
            )

        uit_object.register_factory(
            http_req_sender=RequestsHTTPRequest(),
            http_resp_parser=RequestsHTTPResponseParser(),
            data_process=ExampleWebDataHandler(),
        )
        try:
            result = uit_object._run_crawling_processing(content=_One_Running_Content_As_Object)
        except:
            assert False, f"It should not occur any issue. The exception: {traceback.format_exc()}"
        else:
            assert True, "It works finely!"
            assert (
                result == "Example Domain"
            ), "It crawling result should be 'Example Domain' (the title of example.com)."


class TestZookeeperCrawlerFeatureWithMultipleCrawlers(MultiCrawlerTestSuite):
    @MultiCrawlerTestSuite._clean_environment
    def test_initial(self):
        running_exception: Dict[str, Optional[Exception]] = _Manager.dict()
        running_flag: Dict[str, bool] = _Manager.dict()
        done_flag = _Manager.Value("done_flag", False)

        def _update_group_state() -> None:
            try:
                time.sleep(1)

                # Update the meta-data *GroupState*
                group_state_data, state = self._get_value_from_node(path=_Testing_Value.group_state_zookeeper_path)
                group_state = json.loads(group_state_data.decode("utf-8"))
                group_state["current_crawler"].extend(
                    [_Testing_Value.name.replace("1", "2"), _Testing_Value.name.replace("1", "3")]
                )
                self._set_value_to_node(
                    path=_Testing_Value.group_state_zookeeper_path, value=bytes(json.dumps(group_state), "utf-8")
                )

                time.sleep(1)

                if done_flag.value is False:
                    raise Exception("Test fail")
            except Exception as e:
                running_flag["_chk_running"] = False
                running_exception["_chk_running"] = e
            else:
                running_flag["_chk_running"] = False
                running_exception["_chk_running"] = None

        def _run_initial() -> None:
            # Instantiate ZookeeperCrawler
            zk_crawler = ZookeeperCrawler(
                runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, initial=False, zk_hosts=Zookeeper_Hosts
            )

            try:
                # Operate target method to test
                zk_crawler.initial()
            except Exception as e:
                running_flag["_run_initial"] = False
                running_exception["_run_initial"] = e
            else:
                running_flag["_run_initial"] = False
                running_exception["_run_initial"] = None
                done_flag.value = True

        run_2_diff_workers(
            func1_ps=(_update_group_state, None, False), func2_ps=(_run_initial, None, True), worker="thread"
        )

        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        assert done_flag.value is True, "The initialing process should be done."

        # Verify
        self._verify_metadata.group_state_is_not_empty(
            runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, standby_id="0"
        )
        self._verify_metadata.node_state_is_not_empty(role=CrawlerRole.RUNNER.value, group=_Testing_Value.group)
        self._verify_metadata.task_is_not_empty()
        self._verify_metadata.heartbeat_is_not_empty()

    @MultiCrawlerTestSuite._clean_environment
    def test_register_metadata_and_elect_with_many_crawler_instances(self):
        running_exception: Dict[str, Optional[Exception]] = _Manager.dict()
        running_flag: Dict[str, bool] = _Manager.dict()
        is_ready_flag: Dict[str, bool] = _Manager.dict()
        election_results: Dict[str, ElectionResult] = _Manager.dict()

        index_sep_char: str = "_"

        def _run_and_test(_name):
            zk_crawler = None
            try:
                # Instantiate ZookeeperCrawler
                zk_crawler = ZookeeperCrawler(
                    runner=_Runner_Crawler_Value,
                    backup=_Backup_Crawler_Value,
                    name=_name,
                    initial=False,
                    zk_hosts=Zookeeper_Hosts,
                )

                # Run target methods
                zk_crawler.ensure_register = True
                zk_crawler.ensure_timeout = 10
                zk_crawler.ensure_wait = 0.5
                zk_crawler.register_metadata()

                # Verify the running result
                zk_crawler_ready = zk_crawler.is_ready_for_election(timeout=10)
                is_ready_flag[_name] = zk_crawler_ready

                # Try to run election
                elect_result = zk_crawler.elect()
                election_results[_name] = elect_result
            except Exception as e:
                running_flag[_name] = False
                running_exception[_name] = e
            else:
                running_flag[_name] = True
                running_exception[_name] = None

        # Run the target methods by multi-threads
        self._processes = run_multi_processes(
            processes_num=_Total_Crawler_Value, target_function=_run_and_test, index_sep_char=index_sep_char
        )

        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        # Verify the running result by the value from Zookeeper
        self._verify_metadata.group_state_current_section(
            runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, verify_runner=False, verify_backup=False
        )
        self._check_is_ready_flags(is_ready_flag)
        self._check_election_results(election_results, index_sep_char)

    @MultiCrawlerTestSuite._clean_environment
    def test_many_crawler_instances_with_initial(self):
        running_exception: Dict[str, Optional[Exception]] = _Manager.dict()
        running_flag: Dict[str, bool] = _Manager.dict()
        role_results: Dict[str, CrawlerRole] = _Manager.dict()

        index_sep_char: str = "_"

        def _run_and_test(_name):
            zk_crawler = None
            try:
                # Instantiate ZookeeperCrawler
                zk_crawler = ZookeeperCrawler(
                    runner=_Runner_Crawler_Value,
                    backup=_Backup_Crawler_Value,
                    name=_name,
                    initial=True,
                    ensure_initial=True,
                    ensure_timeout=10,
                    ensure_wait=0.5,
                    zk_hosts=Zookeeper_Hosts,
                )

                # Get the instance role of the crawler cluster
                role_results[_name] = zk_crawler.role
            except Exception as e:
                running_flag[_name] = False
                running_exception[_name] = e
            else:
                running_flag[_name] = True
                running_exception[_name] = None

        # Run the target methods by multi-threads
        self._processes = run_multi_processes(
            processes_num=_Total_Crawler_Value, target_function=_run_and_test, index_sep_char=index_sep_char
        )

        # Verify the running state
        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        # Verify the heartbeat info
        self._verify_metadata.all_heartbeat_info(runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value)

        # Verify the running result by the value from Zookeeper
        self._verify_metadata.group_state_info(
            runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, standby_id="3"
        )
        self._check_role(role_results, index_sep_char)

    @MultiCrawlerTestSuite._clean_environment
    def test_running_as_role_runner(self):
        running_exception: Dict[str, Optional[Exception]] = _Manager.dict()
        running_flag: Dict[str, bool] = _Manager.dict()

        def _assign_task() -> None:
            try:
                time.sleep(2)
                task = Initial.task()
                updated_task = Update.task(task, running_content=_Task_Running_Content_Value)
                updated_task_str = json.dumps(updated_task.to_readable_object())
                self._set_value_to_node(path=_Testing_Value.task_zookeeper_path, value=bytes(updated_task_str, "utf-8"))

                node_state = Initial.node_state(group=_Testing_Value.group, role=CrawlerRole.RUNNER)
                node_state_str = json.dumps(node_state.to_readable_object())
                self._set_value_to_node(
                    path=_Testing_Value.node_state_zookeeper_path, value=bytes(node_state_str, "utf-8")
                )
            except Exception as e:
                running_flag["_assign_task"] = False
                running_exception["_assign_task"] = e
            else:
                running_flag["_assign_task"] = True
                running_exception["_assign_task"] = None

        def _wait_for_task() -> None:
            # Instantiate ZookeeperCrawler
            zk_crawler = ZookeeperCrawler(
                name=_Testing_Value.name,
                runner=_Runner_Crawler_Value,
                backup=_Backup_Crawler_Value,
                initial=False,
                zk_hosts=Zookeeper_Hosts,
            )
            zk_crawler.register.node_state()
            zk_crawler.register.task()

            try:
                zk_crawler.running_as_role(CrawlerRole.RUNNER)
            except NotImplementedError:
                assert True, "It works finely."
            else:
                assert (
                    False
                ), "It should raise an error about *NotImplementedError* of registering SmoothCrawler components."

            try:
                zk_crawler.register_factory(
                    http_req_sender=RequestsHTTPRequest(),
                    http_resp_parser=RequestsHTTPResponseParser(),
                    data_process=ExampleWebDataHandler(),
                )
                zk_crawler.running_as_role(CrawlerRole.RUNNER)
            except Exception as e:
                running_flag["_wait_for_task"] = False
                running_exception["_wait_for_task"] = e
            else:
                running_flag["_wait_for_task"] = True
                running_exception["_wait_for_task"] = None

        run_2_diff_workers(func1_ps=(_assign_task, (), False), func2_ps=(_wait_for_task, (), True), worker="thread")

        time.sleep(3)

        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        # Verify the running result
        task_data, state = self._get_value_from_node(path=_Testing_Value.task_zookeeper_path)
        print(f"[DEBUG in testing] _Testing_Value.task_zookeeper_path: {_Testing_Value.task_zookeeper_path}")
        print(f"[DEBUG in testing] task_data: {task_data}")
        self._verify_metadata.one_task_info(
            task_data,
            in_progressing_id="-1",
            running_result={"success_count": 1, "fail_count": 0},
            running_status=TaskState.DONE.value,
            result_detail_len=1,
        )
        self._verify_metadata.one_task_result_detail(
            task_data, task_path=_Testing_Value.task_zookeeper_path, expected_task_result={"1": "available"}
        )

    @MultiCrawlerTestSuite._clean_environment
    def test_wait_for_task_with_dead_role(self):
        running_exception: Dict[str, Optional[Exception]] = _Manager.dict()
        running_flag: Dict[str, bool] = _Manager.dict()

        def _assign_task() -> None:
            try:
                time.sleep(2)
                node_state = Initial.node_state(group=_Testing_Value.group, role=CrawlerRole.DEAD_RUNNER)
                node_state_str = json.dumps(node_state.to_readable_object())
                self._set_value_to_node(
                    path=_Testing_Value.node_state_zookeeper_path, value=bytes(node_state_str, "utf-8")
                )

                task = Initial.task()
                updated_task = Update.task(task, running_content=_Task_Running_Content_Value)
                updated_task_str = json.dumps(updated_task.to_readable_object())
                self._set_value_to_node(path=_Testing_Value.task_zookeeper_path, value=bytes(updated_task_str, "utf-8"))
            except Exception as e:
                running_flag["_assign_task"] = False
                running_exception["_assign_task"] = e
            else:
                running_flag["_assign_task"] = True
                running_exception["_assign_task"] = None

        def _wait_for_task() -> None:
            # Instantiate ZookeeperCrawler
            zk_crawler = ZookeeperCrawler(
                name=_Testing_Value.name,
                runner=_Runner_Crawler_Value,
                backup=_Backup_Crawler_Value,
                initial=False,
                zk_hosts=Zookeeper_Hosts,
            )
            zk_crawler.register.node_state()
            zk_crawler.register.task()

            try:
                zk_crawler.running_as_role(CrawlerRole.RUNNER)
            except NotImplementedError:
                assert True, "It works finely."
            else:
                assert (
                    False
                ), "It should raise an error about *NotImplementedError* of registering SmoothCrawler components."

            try:
                zk_crawler.register_factory(
                    http_req_sender=RequestsHTTPRequest(),
                    http_resp_parser=RequestsHTTPResponseParser(),
                    data_process=ExampleWebDataHandler(),
                )
                zk_crawler.running_as_role(CrawlerRole.RUNNER)
            except Exception as e:
                running_flag["_wait_for_task"] = False
                running_exception["_wait_for_task"] = e
            else:
                running_flag["_wait_for_task"] = True
                running_exception["_wait_for_task"] = None

        run_2_diff_workers(func1_ps=(_assign_task, (), False), func2_ps=(_wait_for_task, (), True), worker="thread")

        time.sleep(3)

        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        # Verify the running result
        task_data, state = self._get_value_from_node(path=_Testing_Value.task_zookeeper_path)
        print(f"[DEBUG in testing] _Testing_Value.task_zookeeper_path: {_Testing_Value.task_zookeeper_path}")
        print(f"[DEBUG in testing] task_data: {task_data}")
        self._verify_metadata.one_task_info(
            task_data,
            in_progressing_id="-1",
            running_result={"success_count": 0, "fail_count": 0},
            running_status=TaskState.NOTHING.value,
            result_detail_len=0,
        )
        self._verify_metadata.one_task_result_detail(
            task_data, task_path=_Testing_Value.task_zookeeper_path, expected_task_result={"1": "dead"}
        )

    @MultiCrawlerTestSuite._clean_environment
    def test_running_as_role_primary_backup(self):
        # Instantiate ZookeeperCrawler
        zk_crawler = ZookeeperCrawler(runner=1, backup=1, initial=False, zk_hosts=Zookeeper_Hosts)

        def _initial_group_state() -> None:
            state = Initial.group_state(
                crawler_name="sc-crawler_0",
                total_crawler=2,
                total_runner=1,
                total_backup=1,
                standby_id="1",
                current_crawler=["sc-crawler_0", "sc-crawler_1"],
                current_runner=["sc-crawler_0"],
                current_backup=["sc-crawler_1"],
            )
            if self._exist_node(path=_Testing_Value.group_state_zookeeper_path) is None:
                state_data_str = json.dumps(state.to_readable_object())
                self._create_node(
                    path=_Testing_Value.group_state_zookeeper_path,
                    value=bytes(state_data_str, "utf-8"),
                    include_data=True,
                )

        def _initial_node_state(_node_path: str) -> None:
            node_state = Initial.node_state(group=zk_crawler.group, role=CrawlerRole.RUNNER)
            if self._exist_node(path=_node_path) is None:
                node_state_data_str = json.dumps(node_state.to_readable_object())
                self._create_node(path=_node_path, value=bytes(node_state_data_str, "utf-8"), include_data=True)

        def _initial_task(_task_path: str) -> None:
            task = Initial.task(running_state=TaskState.PROCESSING)
            if self._exist_node(path=_task_path) is None:
                task_data_str = json.dumps(task.to_readable_object())
                self._create_node(path=_task_path, value=bytes(task_data_str, "utf-8"), include_data=True)

        def _initial_heartbeat(_heartbeat_path: str) -> None:
            heartbeat = Initial.heartbeat()
            if self._exist_node(path=_heartbeat_path) is None:
                heartbeat_data_str = json.dumps(heartbeat.to_readable_object())
                self._create_node(path=_heartbeat_path, value=bytes(heartbeat_data_str, "utf-8"), include_data=True)

        _initial_group_state()

        # Set a *NodeState* of all crawler instances
        all_node_state_paths = _ZKNodePathUtils.all_node_state(size=2, start_index=0)
        for node_state_path in all_node_state_paths:
            _initial_node_state(node_state_path)

        # Set a *Task* of all crawler instances
        all_task_paths = _ZKNodePathUtils.all_task(size=2, start_index=0)
        for task_path in all_task_paths:
            _initial_task(task_path)

        # Set a *Task* of all crawler instances
        all_heartbeat_paths = _ZKNodePathUtils.all_heartbeat(size=2, start_index=0)
        for heartbeat_path in all_heartbeat_paths:
            _initial_heartbeat(heartbeat_path)

        if zk_crawler.is_ready_for_run(timeout=5):
            zk_crawler.running_as_role(CrawlerRole.BACKUP_RUNNER)
        else:
            assert False, (
                "It should be ready to run. Please check the detail implementation or other settings in testing has "
                "problem or not."
            )

        # Verify the result should be correct as expected
        # Verify the *GroupState* info
        self._verify_metadata.group_state_info(runner=1, backup=1, fail_runner=1, standby_id="2")

        # Verify the *NodeState* info
        self._verify_metadata.all_node_state_role(
            runner=1,
            backup=1,
            expected_role={"0": CrawlerRole.DEAD_RUNNER, "1": CrawlerRole.RUNNER},
            expected_group={"0": zk_crawler._crawler_group, "1": zk_crawler._crawler_group},
            start_index=0,
        )

    @MultiCrawlerTestSuite._clean_environment
    def test_running_as_role_secondary_backup(self):
        running_exception: Dict[str, Optional[Exception]] = _Manager.dict()
        running_flag: Dict[str, bool] = _Manager.dict()

        # # Prepare the meta data
        # Instantiate a ZookeeperCrawler for testing
        zk_crawler = ZookeeperCrawler(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            name="sc-crawler_2",
            initial=False,
            zk_hosts=Zookeeper_Hosts,
        )

        # Set a *State* with only 2 crawlers and standby ID is '1'
        state = Initial.group_state(
            crawler_name="sc-crawler_0",
            total_crawler=3,
            total_runner=2,
            total_backup=1,
            standby_id="1",
            current_crawler=["sc-crawler_0", "sc-crawler_1", "sc-crawler_2"],
            current_runner=["sc-crawler_0"],
            current_backup=["sc-crawler_1", "sc-crawler_2"],
        )
        if self._exist_node(path=zk_crawler._zk_path.group_state) is None:
            state_data_str = json.dumps(state.to_readable_object())
            self._create_node(
                path=zk_crawler._zk_path.group_state,
                value=bytes(state_data_str, "utf-8"),
                include_data=True,
            )

        result = _Manager.Value("result", None)
        start = _Manager.Value("start", None)
        end = _Manager.Value("end", None)

        def _update_state_standby_id():
            try:
                time.sleep(5)
                state.standby_id = "2"
                zk_crawler._metadata_util.set_metadata_to_zookeeper(
                    path=zk_crawler._zk_path.group_state, metadata=state
                )
            except Exception as e:
                running_flag["_update_state_standby_id"] = False
                running_exception["_update_state_standby_id"] = e
            else:
                running_flag["_update_state_standby_id"] = True
                running_exception["_update_state_standby_id"] = None

        def _run_target_test_func():
            try:
                nonlocal result, start, end
                start.value = time.time()
                # # Run target function
                result.value = zk_crawler.running_as_role(CrawlerRole.BACKUP_RUNNER)
                end.value = time.time()
            except Exception as e:
                running_flag["_run_target_test_func"] = False
                running_exception["_run_target_test_func"] = e
            else:
                running_flag["_run_target_test_func"] = True
                running_exception["_run_target_test_func"] = None
            finally:
                if self._exist_node(path=zk_crawler._zk_path.group_state):
                    self._delete_node(path=zk_crawler._zk_path.group_state)

        run_2_diff_workers(
            func1_ps=(_update_state_standby_id, (), False), func2_ps=(_run_target_test_func, (), False), worker="thread"
        )

        # # Verify the result
        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        assert result.value is True, "It should be True after it detect the stand ID to be '2'."
        assert 5 < int(end.value - start.value) <= 6, "It should NOT run more than 6 seconds."

    def _check_is_ready_flags(self, is_ready_flag: Dict[str, bool]) -> None:
        assert (
            len(is_ready_flag.keys()) == _Total_Crawler_Value
        ), f"The size of *is_ready* feature checksum should be {_Total_Crawler_Value}."
        assert (
            False not in is_ready_flag
        ), "It should NOT exist any checksum element is False (it means crawler doesn't ready for running election)."

    def _check_election_results(self, election_results: Dict[str, ElectionResult], index_sep_char: str) -> None:
        assert (
            len(election_results.keys()) == _Total_Crawler_Value
        ), f"The size of *elect* feature checksum should be {_Total_Crawler_Value}."
        for crawler_name, election_result in election_results.items():
            crawler_index = int(crawler_name.split(index_sep_char)[-1])
            if crawler_index <= _Runner_Crawler_Value:
                assert (
                    election_result is ElectionResult.WINNER
                ), f"The election result of '{crawler_name}' should be *ElectionResult.Winner*."
            else:
                assert (
                    election_result is ElectionResult.LOSER
                ), f"The election result of '{crawler_name}' should be *ElectionResult.Loser*."

    def _check_role(self, role_results: Dict[str, CrawlerRole], index_sep_char: str) -> None:
        assert (
            len(role_results.keys()) == _Total_Crawler_Value
        ), f"The size of *role* attribute checksum should be {_Total_Crawler_Value}."
        for crawler_name, role in role_results.items():
            crawler_index = int(crawler_name.split(index_sep_char)[-1])
            if crawler_index <= _Runner_Crawler_Value:
                assert (
                    role is CrawlerRole.RUNNER
                ), f"The role of this crawler instance '{crawler_name}' should be '{CrawlerRole.RUNNER}'."
            else:
                assert (
                    role is CrawlerRole.BACKUP_RUNNER
                ), f"The role of this crawler instance '{crawler_name}' should be '{CrawlerRole.BACKUP_RUNNER}'."


class TestZookeeperCrawlerRunUnderDiffScenarios(MultiCrawlerTestSuite):
    @MultiCrawlerTestSuite._clean_environment
    def test_run_with_one_backup(self):
        # Run the target methods by multi-processes
        running_exception, running_flag, role_results = self._run_multiple_crawler_instances(
            runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, delay=3, delay_assign_task=2
        )

        time.sleep(7)  # Wait for thread 2 dead and thread 3 activate itself to be runner.

        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        # Verify running result from meta-data
        self._verify_results(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            fail_runner=_Fail_Runner_Crawler_Value,
            expected_role={
                "1": CrawlerRole.RUNNER,
                "2": CrawlerRole.DEAD_RUNNER,
                "3": CrawlerRole.RUNNER,
            },
            expected_group={"1": "sc-crawler-cluster", "2": "sc-crawler-cluster", "3": "sc-crawler-cluster"},
            expected_task_result={"1": "available", "2": "available", "3": "available"},
        )

    @MultiCrawlerTestSuite._clean_environment
    def test_run_with_multiple_backup(self):
        _Multiple_Backup_Scenarios_Runner_Crawler: int = 2
        _Multiple_Backup_Scenarios_Backup_Crawler: int = 2

        # Run the target methods by multi-processes
        running_exception, running_flag, role_results = self._run_multiple_crawler_instances(
            runner=_Multiple_Backup_Scenarios_Runner_Crawler,
            backup=_Multiple_Backup_Scenarios_Backup_Crawler,
            delay=3,
            delay_assign_task=4,
        )

        time.sleep(7)  # Wait for thread 2 dead and thread 3 activate itself to be runner.

        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        # Verify running result from meta-data
        self._verify_results(
            runner=_Multiple_Backup_Scenarios_Runner_Crawler,
            backup=_Multiple_Backup_Scenarios_Backup_Crawler,
            fail_runner=_Fail_Runner_Crawler_Value,
            expected_role={
                "1": CrawlerRole.RUNNER,
                "2": CrawlerRole.DEAD_RUNNER,
                "3": CrawlerRole.RUNNER,
                "4": CrawlerRole.BACKUP_RUNNER,
            },
            expected_group={
                "1": "sc-crawler-cluster",
                "2": "sc-crawler-cluster",
                "3": "sc-crawler-cluster",
                "4": "sc-crawler-cluster",
            },
            expected_task_result={"1": "available", "2": "available", "3": "available", "4": "backup"},
        )

    @MultiCrawlerTestSuite._clean_environment
    def test_run_with_multiple_fail_runners(self):
        _Multiple_Fail_Scenarios_Runner_Crawler: int = 3
        _Multiple_Fail_Scenarios_Backup_Crawler: int = 3
        _Multiple_Fail_Scenarios_Fail_Crawler: int = 2

        # Run the target methods by multi-processes
        running_exception, running_flag, role_results = self._run_multiple_crawler_instances(
            runner=_Multiple_Fail_Scenarios_Runner_Crawler,
            backup=_Multiple_Fail_Scenarios_Backup_Crawler,
            multi_fail=_Multiple_Fail_Scenarios_Fail_Crawler,
            delay=3,
            delay_assign_task=4,
        )

        time.sleep(7)  # Wait for thread 2 dead and thread 3 activate itself to be runner.

        self._verify.exception(running_exception)
        self._verify.running_status(running_flag)

        # Verify running result from meta-data
        self._verify_results(
            runner=_Multiple_Fail_Scenarios_Runner_Crawler,
            backup=_Multiple_Fail_Scenarios_Backup_Crawler,
            fail_runner=_Multiple_Fail_Scenarios_Fail_Crawler,
            expected_role={
                "1": CrawlerRole.DEAD_RUNNER,
                "2": CrawlerRole.DEAD_RUNNER,
                "3": CrawlerRole.RUNNER,
                "4": CrawlerRole.RUNNER,
                "5": CrawlerRole.RUNNER,
                "6": CrawlerRole.BACKUP_RUNNER,
            },
            expected_group={
                "1": "sc-crawler-cluster",
                "2": "sc-crawler-cluster",
                "3": "sc-crawler-cluster",
                "4": "sc-crawler-cluster",
                "5": "sc-crawler-cluster",
                "6": "sc-crawler-cluster",
            },
            expected_task_result={
                "1": "available",
                "2": "available",
                "3": "available",
                "4": "available",
                "5": "backup",
                "6": "backup",
            },
        )

    def _run_multiple_crawler_instances(
        self,
        runner: int,
        backup: int,
        delay: Union[bool, int] = False,
        delay_assign_task: int = None,
        multi_fail: int = None,
    ) -> (dict, dict, dict):
        fail_index_criteria = None
        if multi_fail is not None:
            assert runner >= 2 and backup >= 2, "Runner and backup all should be more than 2 instances."
            fail_index_criteria = multi_fail + 1

        running_exception: Dict[str, Optional[Exception]] = _Manager.dict()
        running_flag: Dict[str, bool] = _Manager.dict()
        role_results: Dict[str, CrawlerRole] = _Manager.dict()

        def _assign_task() -> None:
            task_paths = _ZKNodePathUtils.all_task(runner)
            for path in task_paths:
                task_running_content = _One_Running_Content
                sleep_arg = ""
                if isinstance(delay, bool):
                    if delay is True and "2" in path:
                        sleep_arg = "?sleep=5"
                elif isinstance(delay, int):
                    sleep_arg = f"?sleep={delay}"
                else:
                    raise ValueError("Argument ** only support boolean and integer type value.")
                _One_Running_Content["url"] = _One_Running_Content["url"].split("?")[0]
                task_running_content["url"] = _One_Running_Content["url"] + sleep_arg
                task_running_contents = [task_running_content]
                task = Initial.task(running_content=task_running_contents)
                if self._exist_node(path) is None:
                    self._create_node(
                        path=path, value=bytes(json.dumps(task.to_readable_object()), "utf-8"), include_data=True
                    )
                else:
                    self._set_value_to_node(path=path, value=bytes(json.dumps(task.to_readable_object()), "utf-8"))

        def _run_and_test(name: str) -> None:
            try:
                # Instantiate ZookeeperCrawler
                if (fail_index_criteria is None and "2" in name) or (
                    fail_index_criteria is not None and int(name.split("_")[-1]) < fail_index_criteria
                ):
                    zk_crawler = ZookeeperCrawler(
                        runner=runner,
                        backup=backup,
                        name=name,
                        initial=False,
                        ensure_initial=True,
                        ensure_timeout=20,
                        ensure_wait=1,
                        zk_hosts=Zookeeper_Hosts,
                    )
                    zk_crawler.stop_update_heartbeat()
                    zk_crawler.initial()
                else:
                    if isinstance(delay, bool) and backup > 1 and str(runner + 1) in name:
                        time.sleep(3)
                    elif isinstance(delay, int) and backup > 1 and str(runner + 1) in name:
                        time.sleep(delay)
                    zk_crawler = ZookeeperCrawler(
                        runner=runner,
                        backup=backup,
                        name=name,
                        initial=True,
                        ensure_initial=True,
                        ensure_timeout=20,
                        ensure_wait=1,
                        zk_hosts=Zookeeper_Hosts,
                    )

                # Get the instance role of the crawler cluster
                role_results[name] = zk_crawler.role

                zk_crawler.register_factory(
                    http_req_sender=RequestsHTTPRequest(),
                    http_resp_parser=RequestsHTTPResponseParser(),
                    data_process=ExampleWebDataHandler(),
                )
                zk_crawler.run()
            except Exception as e:
                running_flag[name] = False
                running_exception[name] = e
            else:
                running_flag[name] = True
                running_exception[name] = None

        self._processes = []
        # Run the target methods by multi-processes
        for i in range(1, (runner + backup + 1)):
            crawler_process = mp.Process(target=_run_and_test, args=(f"sc-crawler_{i}",))
            crawler_process.daemon = True
            self._processes.append(crawler_process)

        for process in self._processes:
            process.start()

        if delay_assign_task is not None:
            time.sleep(delay_assign_task)
        _assign_task()

        return running_exception, running_flag, role_results

    def _verify_results(
        self,
        runner: int,
        backup: int,
        fail_runner: int,
        expected_role: dict,
        expected_group: dict,
        expected_task_result: Dict[str, Union[str, List[str]]],
    ) -> None:
        # Verify the group info
        self._verify_metadata.group_state_info(
            runner=runner, backup=backup, fail_runner=fail_runner, standby_id=str(runner + fail_runner + 1)
        )
        # Verify the state info
        self._verify_metadata.all_node_state_role(
            runner=runner, backup=backup, expected_role=expected_role, expected_group=expected_group
        )
        # Verify the task info
        self._verify_metadata.all_task_detail(runner=runner, backup=backup, expected_task_result=expected_task_result)
