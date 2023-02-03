import traceback
from abc import ABCMeta, abstractmethod
from typing import Union

import pytest

from smoothcrawler_cluster.crawler.adapter import DistributedLock

_ENTER_FLAG = False
_EXIST_FLAG = False


def _reset_flags() -> None:
    global _ENTER_FLAG, _EXIST_FLAG
    _ENTER_FLAG = False
    _EXIST_FLAG = False


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
    @staticmethod
    def _reset_flags(function):
        def _(self, *args, **kwargs):
            _reset_flags()
            try:
                # Prevent to the flags doesn't be reset finely.
                global _ENTER_FLAG, _EXIST_FLAG
                assert _ENTER_FLAG is False, "Before run function, initialed flag '_ENTER_FLAG' should be False."
                assert _EXIST_FLAG is False, "Before run function, initialed flag '_EXIT_FLAG' should be False."

                # Truly run the testing
                function(self, *args, **kwargs)
            finally:
                _reset_flags()

        return _

    def template_testing_run(self, ut_lock: DistributedLock) -> None:
        self._test_run_by_function_without_args(lock=ut_lock)
        self._test_run_by_function_with_args(lock=ut_lock)
        self._test_run_by_function_with_kwargs(lock=ut_lock)

    def template_testing_run_with_lock(
        self, ut_lock: DistributedLock, test_obj_has_special_method: bool = True
    ) -> None:
        self._test_run_with_lock_by_function_without_args(ut_lock, test_obj_has_special_method)
        self._test_run_with_lock_by_function_with_args(ut_lock, test_obj_has_special_method)
        self._test_run_with_lock_by_function_with_kwargs(ut_lock, test_obj_has_special_method)

    @_reset_flags
    def template_testing_has_enter_and_exit(self, ut_lock: DistributedLock, test_obj_has_special_method: bool) -> None:
        has_with = ut_lock.has_enter_or_exist()
        if test_obj_has_special_method:
            assert (
                has_with is True
            ), f"The check result should be True if under test object {ut_lock} has special methods."
        else:
            assert (
                has_with is False
            ), f"The check result should be False if under test object {ut_lock} doesn't have special methods."

    @_reset_flags
    def _test_run_by_function_without_args(self, lock: DistributedLock) -> None:
        return_val = lock.run(function=_test_func)
        self._verify(return_val, _NO_ARGS_RETURN_VALUE)

    @_reset_flags
    def _test_run_by_function_with_args(self, lock: DistributedLock) -> None:
        args = ("func_test",)
        return_val = lock.run(_test_func, *args)
        self._verify(return_val, args)

    @_reset_flags
    def _test_run_by_function_with_kwargs(self, lock: DistributedLock) -> None:
        kwargs = {"func_test": "func_test_val"}
        return_val = lock.run(_test_func, **kwargs)
        self._verify(return_val, kwargs)

    @_reset_flags
    def _test_run_with_lock_by_function_without_args(
        self, lock: DistributedLock, test_obj_has_special_method: bool = True
    ) -> None:
        try:
            return_val = lock.run_in_lock(function=_test_func)
        except Exception as e:
            if test_obj_has_special_method:
                self._verify_exception(e)
            assert False, traceback.format_exception(e)
        else:
            self._verify(return_val, _NO_ARGS_RETURN_VALUE)

    @_reset_flags
    def _test_run_with_lock_by_function_with_args(
        self, lock: DistributedLock, test_obj_has_special_method: bool = True
    ) -> None:
        args = ("func_test",)
        try:
            return_val = lock.run_in_lock(_test_func, *args)
        except Exception as e:
            if test_obj_has_special_method:
                self._verify_exception(e)
            assert False, traceback.format_exception(e)
        else:
            self._verify(return_val, args)

    @_reset_flags
    def _test_run_with_lock_by_function_with_kwargs(
        self, lock: DistributedLock, test_obj_has_special_method: bool = True
    ) -> None:
        kwargs = {"func_test": "func_test_val"}
        try:
            return_val = lock.run_in_lock(_test_func, **kwargs)
        except Exception as e:
            if test_obj_has_special_method:
                self._verify_exception(e)
            assert False, traceback.format_exception(e)
        else:
            self._verify(return_val, kwargs)

    @classmethod
    def _verify(cls, return_val, expected_val) -> None:
        global _ENTER_FLAG, _EXIST_FLAG
        assert return_val == expected_val, f"It should return {expected_val} result. But it got {return_val}."
        assert _ENTER_FLAG is True, "It should run __enter__ function."
        assert _EXIST_FLAG is True, "It should run __exist__ function."

    @classmethod
    def _verify_exception(cls, exc: Exception) -> None:
        assert (
            type(exc) is NotImplementedError
        ), "It should raise *NotImplementedError* if lock function doesn't have special methods *__enter__* and *__exit__*."


class TestDistributedLockByWithObj(DistributedLockTestSpec):
    @pytest.fixture(scope="function")
    def lock(self) -> DistributedLock:
        return DistributedLock(lock=_MockWithObj)

    @pytest.fixture(scope="function")
    def lock_with_args(self) -> DistributedLock:
        args = ("test",)
        return DistributedLock(_MockWithObj, *args)

    @pytest.fixture(scope="function")
    def lock_with_kwargs(self) -> DistributedLock:
        kwargs = {"test": "test_val"}
        return DistributedLock(lock=_MockWithObj, **kwargs)

    def test_run_by_lock(self, lock: DistributedLock):
        self.template_testing_run(lock)

    def test_run_in_lock_by_lock(self, lock: DistributedLock):
        self.template_testing_run_with_lock(lock)

    def test_has_enter_or_exist_by_lock(self, lock: DistributedLock):
        self.template_testing_has_enter_and_exit(ut_lock=lock, test_obj_has_special_method=True)

    def test_run_by_lock_with_args(self, lock_with_args: DistributedLock):
        self.template_testing_run(lock_with_args)

    def test_run_in_lock_by_lock_with_args(self, lock_with_args: DistributedLock):
        self.template_testing_run_with_lock(lock_with_args)

    def test_has_enter_or_exist_by_lock_with_args(self, lock_with_args: DistributedLock):
        self.template_testing_has_enter_and_exit(ut_lock=lock_with_args, test_obj_has_special_method=True)

    def test_run_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        self.template_testing_run(lock_with_kwargs)

    def test_run_in_lock_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        self.template_testing_run_with_lock(lock_with_kwargs)

    def test_has_enter_or_exist_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        self.template_testing_has_enter_and_exit(ut_lock=lock_with_kwargs, test_obj_has_special_method=True)
