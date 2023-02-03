from typing import Union

import pytest

from smoothcrawler_cluster.crawler.adapter import DistributedLock

_ENTER_FLAG = None
_EXIST_FLAG = None


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


class TestDistributedLockByWithObj:
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
        _reset_flags()
        return_val = lock.run(function=_test_func)
        self._verify(return_val, _NO_ARGS_RETURN_VALUE)

        _reset_flags()
        args = ("func_test",)
        return_val = lock.run(_test_func, *args)
        self._verify(return_val, args)

        _reset_flags()
        kwargs = {"func_test": "func_test_val"}
        return_val = lock.run(_test_func, **kwargs)
        self._verify(return_val, kwargs)

    def test_run_in_lock_by_lock(self, lock: DistributedLock):
        _reset_flags()
        return_val = lock.run_in_lock(function=_test_func)
        self._verify(return_val, _NO_ARGS_RETURN_VALUE)

        _reset_flags()
        args = ("func_test",)
        return_val = lock.run_in_lock(_test_func, *args)
        self._verify(return_val, args)

        _reset_flags()
        kwargs = {"func_test": "func_test_val"}
        return_val = lock.run_in_lock(_test_func, **kwargs)
        self._verify(return_val, kwargs)

    def test_has_enter_or_exist_by_lock(self, lock: DistributedLock):
        _reset_flags()
        has_with = lock.has_enter_or_exist()
        assert has_with is True, ""

    def test_run_by_lock_with_args(self, lock_with_args: DistributedLock):
        _reset_flags()
        return_val = lock_with_args.run(function=_test_func)
        self._verify(return_val, _NO_ARGS_RETURN_VALUE)

        _reset_flags()
        args = ("func_test",)
        return_val = lock_with_args.run(_test_func, *args)
        self._verify(return_val, args)

        _reset_flags()
        kwargs = {"func_test": "func_test_val"}
        return_val = lock_with_args.run(_test_func, **kwargs)
        self._verify(return_val, kwargs)

    def test_run_in_lock_by_lock_with_args(self, lock_with_args: DistributedLock):
        _reset_flags()
        return_val = lock_with_args.run_in_lock(function=_test_func)
        self._verify(return_val, _NO_ARGS_RETURN_VALUE)

        _reset_flags()
        args = ("func_test",)
        return_val = lock_with_args.run_in_lock(_test_func, *args)
        self._verify(return_val, args)

        _reset_flags()
        kwargs = {"func_test": "func_test_val"}
        return_val = lock_with_args.run_in_lock(_test_func, **kwargs)
        self._verify(return_val, kwargs)

    def test_has_enter_or_exist_by_lock_with_args(self, lock_with_args: DistributedLock):
        _reset_flags()
        has_with = lock_with_args.has_enter_or_exist()
        assert has_with is True, ""

    def test_run_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        _reset_flags()
        return_val = lock_with_kwargs.run(function=_test_func)
        self._verify(return_val, _NO_ARGS_RETURN_VALUE)

        _reset_flags()
        args = ("func_test",)
        return_val = lock_with_kwargs.run(_test_func, *args)
        self._verify(return_val, args)

        _reset_flags()
        kwargs = {"func_test": "func_test_val"}
        return_val = lock_with_kwargs.run(function=_test_func, **kwargs)
        self._verify(return_val, kwargs)

    def test_run_in_lock_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        _reset_flags()
        return_val = lock_with_kwargs.run_in_lock(function=_test_func)
        self._verify(return_val, _NO_ARGS_RETURN_VALUE)

        _reset_flags()
        args = ("func_test",)
        return_val = lock_with_kwargs.run_in_lock(_test_func, *args)
        self._verify(return_val, args)

        _reset_flags()
        kwargs = {"func_test": "func_test_val"}
        return_val = lock_with_kwargs.run_in_lock(function=_test_func, **kwargs)
        self._verify(return_val, kwargs)

    def test_has_enter_or_exist_by_lock_with_kwargs(self, lock_with_kwargs: DistributedLock):
        _reset_flags()
        has_with = lock_with_kwargs.has_enter_or_exist()
        assert has_with is True, ""

    @classmethod
    def _verify(cls, return_val, expected_val) -> None:
        global _ENTER_FLAG, _EXIST_FLAG
        assert return_val == expected_val, f"It should return {expected_val} result. But it got {return_val}."
        assert _ENTER_FLAG is True, "It should run __enter__ function."
        assert _EXIST_FLAG is True, "It should run __exist__ function."
