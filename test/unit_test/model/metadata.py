from smoothcrawler_cluster.model.metadata import GroupState, NodeState, Task, RunningContent, RunningResult, ResultDetail, Heartbeat
from smoothcrawler_cluster.model.metadata_enum import CrawlerStateRole, TaskResult, HeartState

from datetime import datetime
from typing import List, Dict, Callable
from enum import Enum
from abc import ABCMeta
import traceback
import pytest
import random


class _MetaDataTest(metaclass=ABCMeta):

    @classmethod
    def _run_property_test(cls, setting_func: Callable, getting_func: Callable, valid_value, invalid_1_value, invalid_2_value) -> None:
        """
        Test for getting and setting the property value. It also try to operate the property with invalid value to test
        about it should NOT work finely without any issue.

        :param setting_func: The function which would set value to the property.
                                         The function would be like below: (for example with property *fail_backup*)

                                         .. code_block: python

                                            def _set_func(state: State, value: List[str]) -> None:
                                                state.fail_backup = state

        :param getting_func: The function which would get value by the property.
                                         The function would be like below: (for example with property *fail_backup*)

                                         .. code_block: python

                                            def _get_func(state: State) -> List[str]:
                                                return state.fail_backup

        :param valid_value: The valid value which could be set to the property.
        :param invalid_1_value: The invalid value which would raise an exception if set it to the property.
        :param invalid_2_value: The second one invalid value.
        :return: None
        """

        assert getting_func() is None, "Default initial value should be None value."

        # Set value with normal value.
        _test_cnt = valid_value
        try:
            setting_func(_test_cnt)
        except Exception:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
        else:
            assert True, "It works finely."
            if isinstance(_test_cnt, Enum) is True:
                assert getting_func() == _test_cnt.value, "The value should be same as it set."
            elif type(_test_cnt) is datetime:
                assert getting_func() == _test_cnt.strftime("%Y-%m-%d %H:%M:%S"), "The value should be same as it set."
            elif isinstance(getattr(_test_cnt, "_fields", None), tuple) is True:
                # Testing for namedtuple type object
                _fields = getattr(_test_cnt, "_fields")
                _current_values = getting_func()
                for _f in _fields:
                    assert getattr(_test_cnt, _f) == _current_values[_f], "The value should be same as it set."
            elif type(_test_cnt) is list and False not in map(lambda _one_test: isinstance(getattr(_one_test, "_fields", None), tuple) is True, _test_cnt):
                # Testing for list type value with namedtuple type elements
                _fields = None
                _current_values = getting_func()
                for _one, _value in zip(_test_cnt, _current_values):
                    if _fields is None:
                        _fields = getattr(_one, "_fields")
                    for _f in _fields:
                        assert getattr(_one, _f) == _value[_f], "The value should be same as it set."
            else:
                assert getting_func() == _test_cnt, "The value should be same as it set."

        # Set value with normal value.
        _test_cnt = invalid_1_value
        try:
            setting_func(_test_cnt)
        except Exception:
            assert True, "It works finely."
        else:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"

        # Set value with normal value.
        _test_cnt = invalid_2_value
        try:
            setting_func(_test_cnt)
        except Exception:
            assert True, "It works finely."
        else:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"


class TestGroupState(_MetaDataTest):
    """Test for all the attributes of **GroupState**."""

    @pytest.fixture(scope="function")
    def state(self) -> GroupState:
        return GroupState()

    def test_total_crawler(self, state: GroupState) -> None:
        """
        Test for the property *total_crawler* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> int:
            return state.total_crawler

        def _set_func(value) -> None:
            state.total_crawler = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=5,
            invalid_1_value="5",
            invalid_2_value=5.5
        )

    def test_total_runner(self, state: GroupState) -> None:
        """
        Test for the property *total_runner* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> int:
            return state.total_runner

        def _set_func(value) -> None:
            state.total_runner = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=5,
            invalid_1_value="5",
            invalid_2_value=5.5
        )

    def test_total_backup(self, state: GroupState) -> None:
        """
        Test for the property *total_backup* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> int:
            return state.total_backup

        def _set_func(value) -> None:
            state.total_backup = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=5,
            invalid_1_value="5",
            invalid_2_value=5.5
        )

    def test_current_crawler(self, state: GroupState) -> None:
        """
        Test for the property *current_crawler* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> List[str]:
            return state.current_crawler

        def _set_func(value) -> None:
            state.current_crawler = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=["spider_1"],
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_current_runner(self, state: GroupState) -> None:
        """
        Test for the property *current_runner* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> List[str]:
            return state.current_runner

        def _set_func(value) -> None:
            state.current_runner = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=["spider_1"],
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_current_backup(self, state: GroupState) -> None:
        """
        Test for the property *current_backup* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> List[str]:
            return state.current_backup

        def _set_func(value) -> None:
            state.current_backup = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=["spider_1"],
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_standby_id(self, state: GroupState) -> None:
        """
        Test for the property *standby_id* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> str:
            return state.standby_id

        def _set_func(value) -> None:
            state.standby_id = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value="1",
            invalid_1_value=5,
            invalid_2_value=["iron_man_1"]
        )

    def test_fail_crawler(self, state: GroupState) -> None:
        """
        Test for the property *fail_crawler* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> List[str]:
            return state.fail_crawler

        def _set_func(value) -> None:
            state.fail_crawler = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=["spider_1"],
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_fail_runner(self, state: GroupState) -> None:
        """
        Test for the property *fail_runner* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> List[str]:
            return state.fail_runner

        def _set_func(value) -> None:
            state.fail_runner = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=["spider_1"],
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_fail_backup(self, state: GroupState) -> None:
        """
        Test for the property *fail_backup* of **State**.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        def _get_func() -> List[str]:
            return state.fail_backup

        def _set_func(value) -> None:
            state.fail_backup = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=["spider_1"],
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )


class TestNodeState(_MetaDataTest):
    """Test for all the attributes of **NodeState**."""

    @pytest.fixture(scope="function")
    def state(self) -> NodeState:
        return NodeState()

    def test_group(self, state: NodeState) -> None:

        def _get_func() -> str:
            return state.group

        def _set_func(value) -> None:
            state.group = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value="test-group",
            invalid_1_value=5,
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_set_role_correctly(self, state: NodeState) -> None:
        """
        Test for the setting process should work finely without any issue because it works with normal value
        like enum object **CrawlerStateRole** or valid string type value like ['runner', 'backup-runner',
        'dead-runner', 'dead-backup-runner'].

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        # Test for setting the property normally. It would choice one value randomly.
        _under_test_value: CrawlerStateRole = random.choice([CrawlerStateRole.RUNNER, CrawlerStateRole.BACKUP_RUNNER, CrawlerStateRole.DEAD_RUNNER, CrawlerStateRole.DEAD_BACKUP_RUNNER])
        try:
            state.role = _under_test_value
        except Exception:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
        else:
            assert True, "It works finely."
            assert state.role == _under_test_value.value, "The value should be same as it set."

        _enum_values = list(map(lambda a: a.value, CrawlerStateRole))
        _under_test_value = random.choice(_enum_values)
        try:
            state.role = "runner"
        except Exception:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
        else:
            assert True, "It works finely."
            assert state.role == "runner", "The value should be same as it set."

    def test_set_role_incorrectly(self, state: NodeState) -> None:
        """
        Test for setting the property *role* with invalid string value.

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        # Test for setting the property with incorrect string type value.
        try:
            state.role = "python-hello-world"
        except Exception:
            assert True, "It works finely."
            assert state.role is None, "The value should be None because it got fail when it set the value."
        else:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"


class TestTask(_MetaDataTest):
    """Test for all the attributes of **Task**."""

    @pytest.fixture(scope="function")
    def task(self) -> Task:
        return Task()

    def test_running_content_with_dict(self, task: Task) -> None:

        def _get_func() -> List[dict]:
            return task.running_content

        def _set_func(value) -> None:
            task.running_content = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=[{"k1": "v1", "k2": "v2", "k3": "v3"}],
            invalid_1_value="5",
            invalid_2_value=["spider_1"]
        )

    def test_running_content_with_RunningContent(self, task: Task) -> None:

        def _get_func() -> List[dict]:
            return task.running_content

        def _set_func(value) -> None:
            task.running_content = value

        _content = RunningContent(task_id="0", method="GET", url="http://test.com", parameters={}, header={}, body={})
        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=[_content],
            invalid_1_value=_content,
            invalid_2_value=["spider_1"]
        )

    def test_cookie(self, task: Task) -> None:

        def _get_func() -> dict:
            return task.cookie

        def _set_func(value) -> None:
            task.cookie = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value={"k1": "v1", "k2": "v2", "k3": "v3"},
            invalid_1_value=5,
            invalid_2_value=[{"k1": "v1", "k2": "v2", "k3": "v3"}]
        )

    def test_authorization(self, task: Task) -> None:

        def _get_func() -> dict:
            return task.authorization

        def _set_func(value) -> None:
            task.authorization = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value={"k1": "v1", "k2": "v2", "k3": "v3"},
            invalid_1_value=5,
            invalid_2_value=[{"k1": "v1", "k2": "v2", "k3": "v3"}]
        )

    def test_in_progressing_id(self, task: Task) -> None:

        def _get_func() -> str:
            return task.in_progressing_id

        def _set_func(value) -> None:
            task.in_progressing_id = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value="1",
            invalid_1_value="test_1",
            invalid_2_value=["test_1"]
        )

    def test_running_result_with_dict(self, task: Task) -> None:

        def _get_func() -> dict:
            return task.running_result

        def _set_func(value) -> None:
            task.running_result = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value={"success_cnt":1, "fail_cnt": 1},
            invalid_1_value=5,
            invalid_2_value="test_1"
        )

    def test_running_result_with_RunningResult(self, task: Task) -> None:

        def _get_func() -> dict:
            return task.running_result

        def _set_func(value) -> None:
            task.running_result = value

        _result = RunningResult(success_count=1, fail_count=0)
        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=_result,
            invalid_1_value=5,
            invalid_2_value=[_result]
        )

    def test_running_status(self, task: Task) -> None:

        def _get_func() -> str:
            return task.running_status

        def _set_func(value) -> None:
            task.running_status = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=TaskResult.PROCESSING,
            invalid_1_value=5,
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_result_detail_with_dict(self, task: Task) -> None:

        def _get_func() -> List[dict]:
            return task.result_detail

        def _set_func(value) -> None:
            task.result_detail = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=[{"k1": "v1", "k2": "v2", "k3": "v3"}],
            invalid_1_value=5,
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_result_detail_with_ResultDetail(self, task: Task) -> None:

        def _get_func() -> List[dict]:
            return task.result_detail

        def _set_func(value) -> None:
            task.result_detail = value

        _detail = ResultDetail(task_id="0", state=TaskResult.DONE.value, status_code=200, response="OK", error_msg=None)
        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=[_detail],
            invalid_1_value=5,
            invalid_2_value=_detail
        )


class TestHeartbeat(_MetaDataTest):
    """Test for all the attributes of **Heartbeat**."""

    @pytest.fixture(scope="function")
    def heartbeat(self) -> Heartbeat:
        return Heartbeat()

    def test_heart_rhythm_time_with_string(self, heartbeat: Heartbeat) -> None:

        def _get_func() -> str:
            return heartbeat.heart_rhythm_time

        def _set_func(value) -> None:
            heartbeat.heart_rhythm_time = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value="2022-07-20 07:46:43",
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_heart_rhythm_time_with_datetime(self, heartbeat: Heartbeat) -> None:

        def _get_func() -> str:
            return heartbeat.heart_rhythm_time

        def _set_func(value) -> None:
            heartbeat.heart_rhythm_time = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=datetime.now(),
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_time_format(self, heartbeat: Heartbeat) -> None:

        def _get_func() -> str:
            return heartbeat.time_format

        def _set_func(value) -> None:
            heartbeat.time_format = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value="%Y-%m-%d %H:%M:%S",
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_update_time(self, heartbeat: Heartbeat) -> None:

        def _get_func() -> str:
            return heartbeat.update_time

        def _set_func(value) -> None:
            heartbeat.update_time = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value="2s",
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_update_timeout(self, heartbeat: Heartbeat) -> None:

        def _get_func() -> str:
            return heartbeat.update_timeout

        def _set_func(value) -> None:
            heartbeat.update_timeout = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value="4s",
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_heart_rhythm_timeout(self, heartbeat: Heartbeat) -> None:

        def _get_func() -> str:
            return heartbeat.heart_rhythm_timeout

        def _set_func(value) -> None:
            heartbeat.heart_rhythm_timeout = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value="3",
            invalid_1_value="5s",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_healthy_state(self, heartbeat: Heartbeat) -> None:

        def _get_func() -> str:
            return heartbeat.healthy_state

        def _set_func(value) -> None:
            heartbeat.healthy_state = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=HeartState.HEALTHY,
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )

    def test_task_state(self, heartbeat: Heartbeat) -> None:

        def _get_func() -> str:
            return heartbeat.task_state

        def _set_func(value) -> None:
            heartbeat.task_state = value

        self._run_property_test(
            getting_func=_get_func,
            setting_func=_set_func,
            valid_value=TaskResult.PROCESSING,
            invalid_1_value="5",
            invalid_2_value={"k1": "v1", "k2": "v2", "k3": "v3"}
        )
