from smoothcrawler_cluster._utils.metadata import CrawlerStateRole, TaskResult, State, Task, Heartbeat

from typing import List, Callable, TypeVar, Generic
from enum import Enum
from abc import ABCMeta
import traceback
import pytest
import random


_EnumT = TypeVar("_EnumT", bound=Enum)


class _EnumObjTest(metaclass=ABCMeta):

    def _run_enum_value_test(self, under_test_enum: Enum, expected_value: str) -> None:
        assert under_test_enum.value == expected_value, f"The value of enum member '{under_test_enum}' should be '{expected_value}'."



class TestCrawlerStateRole(_EnumObjTest):
    """
    Test for the enum object key-value mapping.
    """

    def test_runner_value(self) -> None:
        _under_test_enum = CrawlerStateRole.Runner
        _expected_value = "runner"
        self._run_enum_value_test(_under_test_enum, _expected_value)


    def test_backup_runner_value(self) -> None:
        _under_test_enum = CrawlerStateRole.Backup_Runner
        _expected_value = "backup-runner"
        self._run_enum_value_test(_under_test_enum, _expected_value)


    def test_dead_runner_value(self) -> None:
        _under_test_enum = CrawlerStateRole.Dead_Runner
        _expected_value = "dead-runner"
        self._run_enum_value_test(_under_test_enum, _expected_value)


    def test_dead_backup_runner_value(self) -> None:
        _under_test_enum = CrawlerStateRole.Dead_Backup_Runner
        _expected_value = "dead-backup-runner"
        self._run_enum_value_test(_under_test_enum, _expected_value)



class TestTaskResult(_EnumObjTest):
    """
    Test for the enum object key-value mapping.
    """

    def test_processing_value(self) -> None:
        _under_test_enum = TaskResult.Processing
        _expected_value = "processing"
        self._run_enum_value_test(_under_test_enum, _expected_value)


    def test_done_value(self) -> None:
        _under_test_enum = TaskResult.Done
        _expected_value = "done"
        self._run_enum_value_test(_under_test_enum, _expected_value)


    def test_terminate_value(self) -> None:
        _under_test_enum = TaskResult.Terminate
        _expected_value = "terminate"
        self._run_enum_value_test(_under_test_enum, _expected_value)


    def test_error_value(self) -> None:
        _under_test_enum = TaskResult.Error
        _expected_value = "error"
        self._run_enum_value_test(_under_test_enum, _expected_value)



class _MetaDataTest(metaclass=ABCMeta):

    def _run_property_test(self, setting_func: Callable, getting_func: Callable, valid_value, invalid_1_value, invalid_2_value) -> None:
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



class TestState(_MetaDataTest):
    """
    Test for all the attributes of
    """

    @pytest.fixture(scope="function")
    def state(self) -> State:
        return State()


    def test_set_role_correctly(self, state: State) -> None:
        """
        Test for the setting process should work finely without any issue because it works with normal value
        like enum object **CrawlerStateRole** or valid string type value like ['runner', 'backup-runner',
        'dead-runner', 'dead-backup-runner'].

        :param state: The instance of the object **State** with nothing settings.
        :return: None
        """

        # Test for setting the property normally. It would choice one value randomly.
        _under_test_value = random.choice([CrawlerStateRole.Runner, CrawlerStateRole.Backup_Runner, CrawlerStateRole.Dead_Runner, CrawlerStateRole.Dead_Backup_Runner])
        try:
            state.role = _under_test_value
        except Exception:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
        else:
            assert True, "It works finely."
            assert state.role == _under_test_value, "The value should be same as it set."

        _enum_values = list(map(lambda a: a.value, CrawlerStateRole))
        _under_test_value = random.choice(_enum_values)
        try:
            state.role = "runner"
        except Exception:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
        else:
            assert True, "It works finely."
            assert state.role == "runner", "The value should be same as it set."


    def test_set_role_incorrectly(self, state: State) -> None:
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


    def test_total_crawler(self, state: State) -> None:
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


    def test_total_runner(self, state: State) -> None:
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


    def test_total_backup(self, state: State) -> None:
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


    def test_current_crawler(self, state: State) -> None:
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


    def test_current_runner(self, state: State) -> None:
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


    def test_current_backup(self, state: State) -> None:
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


    def test_standby_id(self, state: State) -> None:
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


    def test_fail_crawler(self, state: State) -> None:
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


    def test_fail_runner(self, state: State) -> None:
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


    def test_fail_backup(self, state: State) -> None:
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



class TestTask(_MetaDataTest):

    @pytest.fixture(scope="function")
    def task(self) -> Task:
        return Task()


    def test_task_content(self, task: Task) -> None:
        pass


    def test_task_result(self, task: Task) -> None:
        pass



class TestHeartbeat(_MetaDataTest):

    @pytest.fixture(scope="function")
    def heartbeat(self) -> Heartbeat:
        return Heartbeat()


    def test_datetime(self, heartbeat: Heartbeat) -> None:
        pass
