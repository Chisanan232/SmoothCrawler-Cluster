import traceback
from abc import ABC, ABCMeta, abstractmethod
from typing import Callable, Union

import pytest

from smoothcrawler_cluster.crawler.adapter import DistributedLock

_ENTER_FLAG = False
_EXIST_FLAG = False


def _reset_flags() -> None:
    global _ENTER_FLAG, _EXIST_FLAG
    _ENTER_FLAG = False
    _EXIST_FLAG = False


def _reset_flags_in_test(function):
    def _(*args, **kwargs):
        _reset_flags()
        try:
            # Prevent to the flags doesn't be reset finely.
            global _ENTER_FLAG, _EXIST_FLAG
            assert _ENTER_FLAG is False, "Before run function, initialed flag '_ENTER_FLAG' should be False."
            assert _EXIST_FLAG is False, "Before run function, initialed flag '_EXIT_FLAG' should be False."

            # Truly run the testing
            function(*args, **kwargs)
        finally:
            _reset_flags()

    return _


_NO_ARGS_RETURN_VALUE = "No argument"


def _test_func(*args, **kwargs) -> Union[tuple, dict, str]:
    if args:
        return args
    elif kwargs:
        return kwargs
    else:
        return _NO_ARGS_RETURN_VALUE


class _MockWithObj:
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs

    def __enter__(self):
        global _ENTER_FLAG
        _ENTER_FLAG = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        global _EXIST_FLAG
        _EXIST_FLAG = True


class _MockNoWithObj:
    def __init__(self, *args, **kwargs):
        self._args = args
        self._kwargs = kwargs


class DistributedLockTestSpec(metaclass=ABCMeta):
    @pytest.fixture(scope="function")
    @abstractmethod
    def lock(self) -> DistributedLock:
        pass

    @pytest.fixture(scope="function")
    @abstractmethod
    def lock_with_args(self) -> DistributedLock:
        pass

    @pytest.fixture(scope="function")
    @abstractmethod
    def lock_with_kwargs(self) -> DistributedLock:
        pass

    @property
    @abstractmethod
    def mock_lock(self) -> Callable:
        pass

    @property
    def under_test_obj_has_special_methods(self) -> bool:
        return hasattr(self.mock_lock, "__enter__") and hasattr(self.mock_lock, "__exit__")

    def template_testing_run(self, ut_lock: DistributedLock) -> None:
        self._test_run_by_function_without_args(lock=ut_lock)
        self._test_run_by_function_with_args(lock=ut_lock)
        self._test_run_by_function_with_kwargs(lock=ut_lock)

    def template_testing_run_with_lock(self, ut_lock: DistributedLock) -> None:
        self._test_run_with_lock_by_function_without_args(ut_lock)
        self._test_run_with_lock_by_function_with_args(ut_lock)
        self._test_run_with_lock_by_function_with_kwargs(ut_lock)

    @_reset_flags_in_test
    def template_testing_has_enter_and_exit(self, ut_lock: DistributedLock) -> None:
        has_with = ut_lock.has_enter_or_exist()
        if self.under_test_obj_has_special_methods:
            assert (
                has_with is True
            ), f"The check result should be True if under test object {ut_lock} has special methods."
        else:
            assert (
                has_with is False
            ), f"The check result should be False if under test object {ut_lock} doesn't have special methods."

    @_reset_flags_in_test
    def _test_run_by_function_without_args(self, lock: DistributedLock) -> None:
        return_val = lock.run(function=_test_func)
        self._verify(return_val, _NO_ARGS_RETURN_VALUE)

    @_reset_flags_in_test
    def _test_run_by_function_with_args(self, lock: DistributedLock) -> None:
        args = ("func_test",)
        return_val = lock.run(_test_func, *args)
        self._verify(return_val, args)

    @_reset_flags_in_test
    def _test_run_by_function_with_kwargs(self, lock: DistributedLock) -> None:
        kwargs = {"func_test": "func_test_val"}
        return_val = lock.run(_test_func, **kwargs)
        self._verify(return_val, kwargs)

    @_reset_flags_in_test
    def _test_run_with_lock_by_function_without_args(self, lock: DistributedLock) -> None:
        try:
            return_val = lock.run_in_lock(function=_test_func)
        except Exception as e:
            if not self.under_test_obj_has_special_methods:
                self._verify_exception(e)
            else:
                assert False, traceback.format_exception(e)
        else:
            self._verify(return_val, _NO_ARGS_RETURN_VALUE)

    @_reset_flags_in_test
    def _test_run_with_lock_by_function_with_args(self, lock: DistributedLock) -> None:
        args = ("func_test",)
        try:
            return_val = lock.run_in_lock(_test_func, *args)
        except Exception as e:
            if not self.under_test_obj_has_special_methods:
                self._verify_exception(e)
            else:
                assert False, traceback.format_exception(e)
        else:
            self._verify(return_val, args)

    @_reset_flags_in_test
    def _test_run_with_lock_by_function_with_kwargs(self, lock: DistributedLock) -> None:
        kwargs = {"func_test": "func_test_val"}
        try:
            return_val = lock.run_in_lock(_test_func, **kwargs)
        except Exception as e:
            if not self.under_test_obj_has_special_methods:
                self._verify_exception(e)
            else:
                assert False, traceback.format_exception(e)
        else:
            self._verify(return_val, kwargs)

    def _verify(self, return_val, expected_val) -> None:
        assert return_val == expected_val, f"It should return {expected_val} result. But it got {return_val}."

        global _ENTER_FLAG, _EXIST_FLAG
        should_run_in_with = self.under_test_obj_has_special_methods
        assertion = "NOT " if should_run_in_with is False else ""
        assert _ENTER_FLAG is should_run_in_with, f"It should {assertion}run __enter__ function."
        assert _EXIST_FLAG is should_run_in_with, f"It should {assertion}run __exist__ function."

    @classmethod
    def _verify_exception(cls, exc: Exception) -> None:
        assert type(exc) is NotImplementedError, (
            "It should raise *NotImplementedError* if lock function doesn't have special methods *__enter__* and "
            "*__exit__*."
        )


class BaseDistributedLockTest(DistributedLockTestSpec, ABC):
    @pytest.fixture(scope="function")
    def lock(self) -> DistributedLock:
        return DistributedLock(lock=self.mock_lock)

    @pytest.fixture(scope="function")
    def lock_with_args(self) -> DistributedLock:
        args = ("test",)
        return DistributedLock(self.mock_lock, *args)

    @pytest.fixture(scope="function")
    def lock_with_kwargs(self) -> DistributedLock:
        kwargs = {"test": "test_val"}
        return DistributedLock(lock=self.mock_lock, **kwargs)

    def test_run_by_lock(self, lock: DistributedLock):
        self.template_testing_run(lock)

    def test_run_in_lock_by_lock(self, lock: DistributedLock):
        self.template_testing_run_with_lock(lock)

    def test_has_enter_or_exist_by_lock(self, lock: DistributedLock):
        self.template_testing_has_enter_and_exit(ut_lock=lock)

    def test_run_by_lock_with_args(self, lock_with_args: DistributedLock):
        self.template_testing_run(lock_with_args)

    def test_run_in_lock_by_lock_with_args(self, lock_with_args: DistributedLock):
        self.template_testing_run_with_lock(lock_with_args)

    def test_has_enter_or_exist_by_lock_with_args(self, lock_with_args: DistributedLock):
        self.template_testing_has_enter_and_exit(ut_lock=lock_with_args)

    def test_run_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        self.template_testing_run(lock_with_kwargs)

    def test_run_in_lock_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        self.template_testing_run_with_lock(lock_with_kwargs)

    def test_has_enter_or_exist_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        self.template_testing_has_enter_and_exit(ut_lock=lock_with_kwargs)


class TestDistributedLockByWithObj(BaseDistributedLockTest):
    @property
    def mock_lock(self) -> Callable:
        return _MockWithObj


class TestDistributedLockByNoWithObj(BaseDistributedLockTest):
    @property
    def mock_lock(self) -> Callable:
        return _MockNoWithObj
