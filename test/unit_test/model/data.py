import re
from abc import ABCMeta, abstractmethod
from typing import Callable

import pytest

from smoothcrawler_cluster.model._data import (
    BasePath,
    CrawlerName,
    CrawlerTimer,
    MetaDataOpt,
    MetaDataPath,
    TimeInterval,
    TimerThreshold,
)

from ..._assertion import ValueFormatAssertion
from ..._values import _Crawler_Group_Name_Value, _Crawler_Name_Value
from ._spec import _MetaDataTest


def _generate_timer_interval() -> TimeInterval:
    return TimeInterval()


def _generate_timer_threshold() -> TimerThreshold:
    return TimerThreshold()


class _CannotStrObj:
    def __str__(self):
        raise ValueError("This object cannot convert to str object.")


class TestCrawlerName(_MetaDataTest):
    @pytest.fixture(scope="function")
    def crawler_name(self) -> CrawlerName:
        return CrawlerName()

    def test_repr(self, crawler_name: CrawlerName):
        crawler_name.group = "pytest"
        crawler_name.base_name = "pytest-crawler"
        crawler_name.id = "1"
        crawler_name.index_separation = "_"

        expected_repr = (
            f"<{crawler_name.__class__.__name__} object(group: {crawler_name.group}, name: {str(crawler_name)})>"
        )
        assert (
            repr(crawler_name) == expected_repr
        ), f"It should be same as expected value format of *__repr___*: '{expected_repr}'."

    def test_str(self, crawler_name: CrawlerName):
        crawler_name.group = "pytest"
        crawler_name.base_name = "pytest-crawler"
        crawler_name.id = "1"
        crawler_name.index_separation = "_"

        expected_str = f"{crawler_name.base_name}{crawler_name.index_separation}{crawler_name.id}"
        assert (
            str(crawler_name) == expected_str
        ), f"It should be same as expected value format of *__str___*: '{expected_str}'."

    def test_group(self, crawler_name: CrawlerName):
        def get_func() -> str:
            return crawler_name.group

        def set_func(value) -> None:
            crawler_name.group = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=_Crawler_Group_Name_Value,
            invalid_1_value=_CannotStrObj(),
        )

    def test_base_name(self, crawler_name: CrawlerName):
        def get_func() -> str:
            return crawler_name.base_name

        def set_func(value) -> None:
            crawler_name.base_name = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=_Crawler_Name_Value,
            invalid_1_value=_CannotStrObj(),
        )

    def test_id(self, crawler_name: CrawlerName):
        def get_func() -> str:
            return crawler_name.id

        def set_func(value) -> None:
            crawler_name.id = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value="2",
            invalid_1_value=_CannotStrObj(),
        )

    def test_index_separation(self, crawler_name: CrawlerName):
        def get_func() -> str:
            return crawler_name.index_separation

        def set_func(value) -> None:
            crawler_name.index_separation = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value="-",
            invalid_1_value=_CannotStrObj(),
        )


class TestCrawlerTimer(_MetaDataTest):
    @pytest.fixture(scope="function")
    def crawler_timer(self) -> CrawlerTimer:
        return CrawlerTimer()

    def test_time_interval(self, crawler_timer: CrawlerTimer):
        def get_func() -> TimeInterval:
            return crawler_timer.time_interval

        def set_func(value) -> None:
            crawler_timer.time_interval = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=_generate_timer_interval(),
            invalid_1_value="5",
            invalid_2_value=5.5,
        )

    def test_time_threshold(self, crawler_timer: CrawlerTimer):
        def get_func() -> TimerThreshold:
            return crawler_timer.threshold

        def set_func(value) -> None:
            crawler_timer.threshold = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=_generate_timer_threshold(),
            invalid_1_value="5",
            invalid_2_value=5.5,
        )


class TestTimeInterval(_MetaDataTest):
    @pytest.fixture(scope="function")
    def timer_interval(self) -> TimeInterval:
        return TimeInterval()

    def test_check_task(self, timer_interval: TimeInterval):
        def get_func() -> float:
            return timer_interval.check_task

        def set_func(value) -> None:
            timer_interval.check_task = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=2.4325436,
            invalid_1_value="test",
            invalid_2_value=["test_list"],
        )

    def test_check_crawler_state(self, timer_interval: TimeInterval):
        def get_func() -> float:
            return timer_interval.check_crawler_state

        def set_func(value) -> None:
            timer_interval.check_crawler_state = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=2.4325436,
            invalid_1_value="test",
            invalid_2_value=["test_list"],
        )

    def test_check_standby_id(self, timer_interval: TimeInterval):
        def get_func() -> float:
            return timer_interval.check_standby_id

        def set_func(value) -> None:
            timer_interval.check_standby_id = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=2.4325436,
            invalid_1_value="test",
            invalid_2_value=["test_list"],
        )


class TestTimerThreshold(_MetaDataTest):
    @pytest.fixture(scope="function")
    def timer_threshold(self) -> TimerThreshold:
        return TimerThreshold()

    def test_reset_timeout(self, timer_threshold: TimerThreshold):
        def get_func() -> int:
            return timer_threshold.reset_timeout

        def set_func(value) -> None:
            timer_threshold.reset_timeout = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=2,
            invalid_1_value="test",
            invalid_2_value=["test_list"],
        )


class TestMetaDataOpt(_MetaDataTest):
    @pytest.fixture(scope="function")
    def metadata_opt(self) -> MetaDataOpt:
        return MetaDataOpt()

    def test_get_callback(self, metadata_opt: MetaDataOpt):
        def get_func() -> Callable:
            return metadata_opt.get_callback

        def set_func(value) -> None:
            metadata_opt.get_callback = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=_generate_timer_interval,
            invalid_1_value="test",
            invalid_2_value=["test_list"],
        )

    def test_set_callback(self, metadata_opt: MetaDataOpt):
        def get_func() -> Callable:
            return metadata_opt.set_callback

        def set_func(value) -> None:
            metadata_opt.set_callback = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=_generate_timer_interval,
            invalid_1_value="test",
            invalid_2_value=["test_list"],
        )

    def test_exist_callback(self, metadata_opt: MetaDataOpt):
        def get_func() -> Callable:
            return metadata_opt.exist_callback

        def set_func(value) -> None:
            metadata_opt.exist_callback = value

        self._run_property_test(
            getting_func=get_func,
            setting_func=set_func,
            valid_value=_generate_timer_interval,
            invalid_1_value="test",
            invalid_2_value=["test_list"],
        )


class BasePathTestSpec(metaclass=ABCMeta):
    @pytest.fixture(scope="function")
    @abstractmethod
    def ut_path(self) -> BasePath:
        pass

    @property
    @abstractmethod
    def group_parent_path(self) -> str:
        pass

    @property
    @abstractmethod
    def node_parent_path(self) -> str:
        pass

    def test_property_group_state(self, ut_path: BasePath):
        # Get value by target method for testing
        path = ut_path.group_state

        # Verify values
        ValueFormatAssertion(target=path, regex=re.escape(self.group_parent_path) + r"/[\w\-_]{1,64}/state")

    def test_property_node_state(self, ut_path: BasePath):
        # Get value by target method for testing
        path = ut_path.node_state

        # Verify values
        ValueFormatAssertion(
            target=path, regex=re.escape(self.node_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/state"
        )

    def test_property_task(self, ut_path: BasePath):
        # Get value by target method for testing
        path = ut_path.task

        # Verify values
        ValueFormatAssertion(
            target=path, regex=re.escape(self.node_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/task"
        )

    def test_property_heartbeat(self, ut_path: BasePath):
        # Get value by target method for testing
        path = ut_path.heartbeat

        # Verify values
        ValueFormatAssertion(
            target=path, regex=re.escape(self.node_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}/heartbeat"
        )

    @pytest.mark.parametrize("is_group", [True, False])
    def test_generate_parent_node(self, ut_path: BasePath, is_group: bool):
        if isinstance(ut_path, MetaDataPath):
            # Get value by target method for testing
            path = ut_path.generate_parent_node(parent_name=_Crawler_Name_Value, is_group=is_group)

            # Verify values
            if is_group:
                ValueFormatAssertion(
                    target=path, regex=re.escape(self.group_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}"
                )
            else:
                ValueFormatAssertion(
                    target=path, regex=re.escape(self.node_parent_path) + r"/[\w\-_]{1,64}[-_]{1}[0-9]{1,10000}"
                )


class TestMetaDataPath(BasePathTestSpec):
    @pytest.fixture(scope="function")
    def ut_path(self) -> MetaDataPath:
        return MetaDataPath(name=_Crawler_Name_Value, group=_Crawler_Group_Name_Value)

    @property
    def group_parent_path(self) -> str:
        return "group"

    @property
    def node_parent_path(self) -> str:
        return "node"
