"""*The adapter of features which would be used in workflow*

This module has adapters of some features like distributed lock, major processing function, etc. Workflow is responsible
for defining processing detail how it works and what details it does. But for some features, e.g., distributed lock, it
may be used and may not be used. In the other words, if the workflow processing with Zookeeper, it absolutely has
distributed lock; if the workflow processing with each by algorithm, i.g., sync up the meta-data with each other by
*gossip* algorithm, it doesn't have and also doesn't need to use distributed lock in the workflow. Therefore, the
responsibility what thing it should do (whether it runs with lock or not, no matter it doesn't have, or it doesn't need)
and how it works would let **adapter** module to handle.
"""

from typing import Any, Callable


class DistributedLock:
    """*Adapter of distributed lock feature*

    This is the adapter of distributed lock. It's responsible for running target function with lock if it needs.
    """

    def __init__(self, lock: Callable, *args, **kwargs):
        """

        Args:
            lock (Callable): The lock function which should have special methods *__enter__* and *__exit__*.
            *args (tuple): The lock function arguments which would be used as **args*.
            **kwargs (dict): The lock function arguments which would be used as ***kwargs*.
        """
        self._lock = lock
        self._args = args
        self._kwargs = kwargs

    def run(self, function: Callable, *args, **kwargs) -> Any:
        """Try to run the target function synchronously with lock. If the lock function doesn't have special methods one
        of *__enter__* and *__exit__*, it would keep running the function directly without lock.

        Args:
            function (Callable): The target function to run.
            *args (tuple): The target function arguments which would be used as **args*.
            **kwargs (dict): The target function arguments which would be used as ***kwargs*.

        Returns:
            The return value of the target function.

        """
        if self.has_enter_or_exist():
            return self._run_within_lock(function, *args, **kwargs)
        return function(*args, **kwargs)

    def run_in_lock(self, function: Callable, *args, **kwargs) -> Any:
        """Try to run the target function synchronously with lock. The lock function must have both of special methods
        *__enter__* and *__exit__*, nor it would raise an exception to it.

        Args:
            function (Callable): The target function to run.
            *args (tuple): The target function arguments which would be used as **args*.
            **kwargs (dict): The target function arguments which would be used as ***kwargs*.

        Returns:
            The return value of the target function.

        Raises:
            NotImplementedError: If the lock function doesn't have special methods *__enter__* and *__exit__*. It would
                raise this exception.

        """
        if self.has_enter_or_exist():
            return self._run_within_lock(function, *args, **kwargs)
        raise NotImplementedError(f"This lock object {self._lock} cannot be run by Python keyword *with*.")

    def _run_within_lock(self, function: Callable, *args, **kwargs) -> Any:
        """Synchronously run the target function with lock and arguments if it has.

        Args:
            function (Callable): The target function to run.
            *args (tuple): The target function arguments which would be used as **args*.
            **kwargs (dict): The target function arguments which would be used as ***kwargs*.

        Returns:
            The return value of the target function.

        """
        if args:
            with self._lock(*args):
                return function(*args, **kwargs)
        elif kwargs:
            with self._lock(**kwargs):
                return function(*args, **kwargs)
        else:
            with self._lock():
                return function(*args, **kwargs)

    def has_enter_or_exist(self) -> bool:
        """Check the target lock function has special methods *__enter__* and *__exit__*. In generally, the lock
        function has these 2 special methods.

        Returns:
            It returns *True* if lock function has special methods *__enter__* and *__exit__*. Nor it returns *False*.

        """
        return hasattr(self._lock, "__enter__") or hasattr(self._lock, "__exit__")
