import json
import multiprocessing as mp
import time
from typing import Dict, Optional

from smoothcrawler_cluster._utils.zookeeper import ZookeeperRecipe
from smoothcrawler_cluster.crawler.adapter import DistributedLock
from smoothcrawler_cluster.crawler.cluster import ZookeeperCrawler
from smoothcrawler_cluster.crawler.workflow import (
    HeartbeatUpdatingWorkflow,
    PrimaryBackupRunnerWorkflow,
    RunnerWorkflow,
    SecondaryBackupRunnerWorkflow,
)
from smoothcrawler_cluster.model import CrawlerStateRole, Initial, TaskResult, Update
from smoothcrawler_cluster.model._data import CrawlerTimer, TimeInterval, TimerThreshold

from ..._config import Zookeeper_Hosts
from ..._sample_components._components import (
    ExampleWebDataHandler,
    RequestsHTTPRequest,
    RequestsHTTPResponseParser,
)
from ..._values import (
    _Backup_Crawler_Value,
    _Runner_Crawler_Value,
    _Task_Running_Content_Value,
)
from ..._verify import VerifyMetaData
from .._test_utils._instance_value import _TestValue, _ZKNodePathUtils
from .._test_utils._multirunner import run_2_diff_workers
from ._spec import MultiCrawlerTestSuite

_Manager = mp.Manager()
_Testing_Value: _TestValue = _TestValue()


def _get_workflow_arguments(zk_crawler: ZookeeperCrawler) -> dict:
    restrict_args = {
        "path": zk_crawler._zk_path.group_state,
        "restrict": ZookeeperRecipe.WRITE_LOCK,
        "identifier": zk_crawler._state_identifier,
    }
    workflow_args = {
        "crawler_name": zk_crawler.name,
        "index_sep": zk_crawler._index_sep,
        "path": zk_crawler._zk_path,
        "get_metadata": zk_crawler._get_metadata,
        "set_metadata": zk_crawler._set_metadata,
        "opt_metadata_with_lock": DistributedLock(lock=zk_crawler._zookeeper_client.restrict, **restrict_args),
        "crawler_process_callback": _mock_crawler_processing_func,
    }
    return workflow_args


def _get_run_arguments() -> CrawlerTimer:
    interval = TimeInterval()
    interval.check_task = 2
    interval.check_crawler_state = 0.5
    interval.check_standby_id = 2

    threshold = TimerThreshold()
    threshold.reset_timeout = 10

    timer = CrawlerTimer()
    timer.time_interval = interval
    timer.threshold = threshold
    return timer


def _mock_crawler_processing_func(*args, **kwargs) -> str:
    return "Example Domain"


class TestRunnerWorkflow(MultiCrawlerTestSuite):

    _verify_metadata = VerifyMetaData()

    @MultiCrawlerTestSuite._clean_environment
    def test_wait_for_task(self):
        running_exception: Dict[str, Optional[Exception]] = _Manager.dict()
        running_flag: Dict[str, bool] = _Manager.dict()

        def _assign_task() -> None:
            try:
                time.sleep(2)
                task = Initial.task()
                updated_task = Update.task(task, running_content=_Task_Running_Content_Value)
                updated_task_str = json.dumps(updated_task.to_readable_object())
                self._set_value_to_node(path=_Testing_Value.task_zookeeper_path, value=bytes(updated_task_str, "utf-8"))

                node_state = Initial.node_state(group=_Testing_Value.group, role=CrawlerStateRole.RUNNER)
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
            zk_crawler.register_node_state()
            zk_crawler.register_task()

            workflow_args = _get_workflow_arguments(zk_crawler)
            workflow = RunnerWorkflow(**workflow_args)

            try:
                workflow.run(timer=_get_run_arguments())
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
                workflow.run(timer=_get_run_arguments())
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
            running_status=TaskResult.DONE.value,
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
                node_state = Initial.node_state(group=_Testing_Value.group, role=CrawlerStateRole.DEAD_RUNNER)
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
            zk_crawler.register_node_state()
            zk_crawler.register_task()

            workflow_args = _get_workflow_arguments(zk_crawler)
            workflow = RunnerWorkflow(**workflow_args)

            try:
                workflow.run(timer=_get_run_arguments())
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
                workflow.run(timer=_get_run_arguments())
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
            running_status=TaskResult.NOTHING.value,
            result_detail_len=0,
        )
        self._verify_metadata.one_task_result_detail(
            task_data, task_path=_Testing_Value.task_zookeeper_path, expected_task_result={"1": "dead"}
        )


class TestPrimaryBackupRunnerWorkflow(MultiCrawlerTestSuite):

    _verify_metadata = VerifyMetaData()

    @MultiCrawlerTestSuite._clean_environment
    def test_wait_for_standby(self):
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
            node_state = Initial.node_state(group=zk_crawler.group, role=CrawlerStateRole.RUNNER)
            if self._exist_node(path=_node_path) is None:
                node_state_data_str = json.dumps(node_state.to_readable_object())
                self._create_node(path=_node_path, value=bytes(node_state_data_str, "utf-8"), include_data=True)

        def _initial_task(_task_path: str) -> None:
            task = Initial.task(running_state=TaskResult.PROCESSING)
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

        workflow_args = _get_workflow_arguments(zk_crawler)
        workflow = PrimaryBackupRunnerWorkflow(**workflow_args)

        if zk_crawler.is_ready_for_run(timeout=5):
            workflow.run(timer=_get_run_arguments())
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
            expected_role={"0": CrawlerStateRole.DEAD_RUNNER, "1": CrawlerStateRole.RUNNER},
            expected_group={"0": zk_crawler._crawler_group, "1": zk_crawler._crawler_group},
            start_index=0,
        )


class TestSecondaryBackupRunnerWorkflow(MultiCrawlerTestSuite):

    _verify_metadata = VerifyMetaData()

    @MultiCrawlerTestSuite._clean_environment
    def test_wait_for_to_be_standby(self):
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
                workflow_args = _get_workflow_arguments(zk_crawler)
                workflow = SecondaryBackupRunnerWorkflow(**workflow_args)
                result.value = workflow.run(timer=_get_run_arguments())

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


class TestHeartbeatUpdatingWorkflow(MultiCrawlerTestSuite):

    _verify_metadata = VerifyMetaData()

    @MultiCrawlerTestSuite._clean_environment
    def test_run(self):
        # Instantiate a ZookeeperCrawler for testing
        zk_crawler = ZookeeperCrawler(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            name="sc-crawler_1",
            initial=False,
            zk_hosts=Zookeeper_Hosts,
        )
        zk_crawler.register_task()
        zk_crawler.register_heartbeat()

        workflow_args = _get_workflow_arguments(zk_crawler)
        workflow = HeartbeatUpdatingWorkflow(**workflow_args)

        def _stop_updating():
            time.sleep(5)
            workflow.stop_heartbeat = False

        def _run_updating_heartbeat():
            workflow.run()

        run_2_diff_workers(
            func1_ps=(_stop_updating, (), False), func2_ps=(_run_updating_heartbeat, (), False), worker="thread"
        )

        test_time = 0
        while True:
            if not workflow.stop_heartbeat:
                self._verify_metadata.one_heartbeat_content_has_changed()
                if test_time > 7:
                    assert False, ""
            else:
                self._verify_metadata.one_heartbeat_content_not_changed()
                break
            test_time += 1
            time.sleep(1)
