"""*Data for inner usage*

Here provides some data objects for inner usage in *SmoothCrawler-Cluster*.
"""
from abc import ABCMeta, abstractmethod
from typing import Callable


class CrawlerName:
    """*Data about crawler's name*"""

    _name: str = None
    _index_sep: str = None

    @property
    def name(self) -> str:
        """:obj:`str`: Properties with both getter and setter for crawler instance's name. In generally, it should be
        unique.
        """
        return self._name

    @name.setter
    def name(self, name: str) -> None:
        self._name = str(name)

    @property
    def index_sep(self) -> str:
        """:obj:`str`: Properties with both getter and setter for parsing to get index info from current crawler
        instance's name.
        """
        return self._index_sep

    @index_sep.setter
    def index_sep(self, index_sep: str) -> None:
        self._index_sep = str(index_sep)


class TimeInterval:
    """*Data about each different time interval*"""

    _check_task: float = None
    _check_crawler_state: float = None
    _check_standby_id: float = None

    @property
    def check_task(self) -> float:
        """:obj:`float`: Properties with both getter and setter. It is the interval of checking whether the current
        crawler receives any task or not. This property for **Runner**.
        """
        return self._check_task

    @check_task.setter
    def check_task(self, wait: float) -> None:
        self._check_task = float(wait)

    @property
    def check_crawler_state(self) -> float:
        """:obj:`float`: Properties with both getter and setter. It is the interval of checking whether it has anyone of
        all current crawlers is dead or not. This property for primary **Backup Runner**.
        """
        return self._check_crawler_state

    @check_crawler_state.setter
    def check_crawler_state(self, wait: float) -> None:
        self._check_crawler_state = float(wait)

    @property
    def check_standby_id(self) -> float:
        """:obj:`float`: Properties with both getter and setter. It is the interval of checking whether the current
        crawler could be the primary backup one or not. This property for secondary **BackupRunner**.
        """
        return self._check_standby_id

    @check_standby_id.setter
    def check_standby_id(self, wait: float) -> None:
        self._check_standby_id = float(wait)


class TimerThreshold:
    """*Data about threshold of time*"""

    _reset_timeout: int = None

    @property
    def reset_timeout(self) -> int:
        """:obj:`int`: Properties with both getter and setter. It is the threshold of reset timeout record for checking
        all the current crawler's heartbeat state. This property for primary **Backup Runner**.
        """
        return self._reset_timeout

    @reset_timeout.setter
    def reset_timeout(self, reset_timeout: int) -> None:
        self._reset_timeout = int(reset_timeout)


class CrawlerTimer:
    """*Data about crawler's time attributes, e.g., time interval, threshold, etc.*"""

    _interval: TimeInterval = None
    _threshold: TimerThreshold = None

    @property
    def time_interval(self) -> TimeInterval:
        """:obj:`TimeInterval`: Properties with both getter and setter. This property is **TimeInterval** object. Please
        refer to :ref:`TimeInterval <InnerData_TimeInterval>` to get more info.
        """
        return self._interval

    @time_interval.setter
    def time_interval(self, interval: TimeInterval) -> None:
        if not isinstance(interval, TimeInterval):
            raise TypeError("Property *time_interval* only support *TimeInterval* type object.")
        self._interval = interval

    @property
    def threshold(self) -> TimerThreshold:
        """:obj:`TimerThreshold`: Properties with both getter and setter. This property is **TimerThreshold** object.
        Please refer to :ref:`TimerThreshold <InnerData_TimerThreshold>` to get more info.
        """
        return self._threshold

    @threshold.setter
    def threshold(self, threshold: TimerThreshold) -> None:
        if not isinstance(threshold, TimerThreshold):
            raise TypeError("Property *time_interval* only support *TimeInterval* type object.")
        self._threshold = threshold


class MetaDataOpt:
    """*Data about callback functions of getting and setting meta-data objects*"""

    _get_callback: Callable = None
    _set_callback: Callable = None

    @property
    def get_callback(self) -> Callable:
        """:obj:`Callable`: Properties with both getter and setter. This is a callback function about getting meta-data
        object.
        """
        return self._get_callback

    @get_callback.setter
    def get_callback(self, callback: Callable) -> None:
        if not isinstance(callback, Callable):
            raise TypeError("Property *get_callback* only support callable object.")
        self._get_callback = callback

    @property
    def set_callback(self) -> Callable:
        """:obj:`Callable`: Properties with both getter and setter. This is a callback function about setting meta-data
        object.
        """
        return self._set_callback

    @set_callback.setter
    def set_callback(self, callback: Callable) -> None:
        if not isinstance(callback, Callable):
            raise TypeError("Property *set_callback* only support callable object.")
        self._set_callback = callback


class BaseNode(metaclass=ABCMeta):
    """*Framework module to define some attributes of node in Zookeeper*

    A node of Zookeeper.
    """

    @property
    @abstractmethod
    def path(self) -> str:
        """:obj:`str`: Properties with both a getter and setter for the path of node in Zookeeper."""
        pass

    @path.setter
    @abstractmethod
    def path(self, val: str) -> None:
        pass

    @property
    @abstractmethod
    def value(self) -> str:
        """:obj:`str`: Properties with both a getter and setter for the value of the path. It may need to deserialize
        the data if it needs.
        """
        pass

    @value.setter
    @abstractmethod
    def value(self, val: str) -> None:
        pass


class BasePath(metaclass=ABCMeta):
    """*Base class to define all meta-data paths*

    All the path properties of meta-data objects.
    """

    def __init__(self, name: str, group: str):
        """

        Args:
            name (str): The name of current crawler instance.
            group (str): The group what current crawler instance is in.
        """
        self._name = name
        self._group = group

    @property
    def state_node_str(self) -> str:
        """:obj:`str`: Property with only getter. The node naming of meta-data **GroupState** and **NodeState** path."""
        return "state"

    @property
    @abstractmethod
    def group_state(self) -> str:
        """:obj:`str`: Property with only getter. The path of meta-data **GroupState**."""
        pass

    @property
    @abstractmethod
    def node_state(self) -> str:
        """:obj:`str`: Property with only getter. The path of meta-data **NodeState**."""
        pass

    @property
    def task_node_str(self) -> str:
        """:obj:`str`: Property with only getter. The node naming of meta-data **Task** path."""
        return "task"

    @property
    @abstractmethod
    def task(self) -> str:
        """:obj:`str`: Property with only getter. The path of meta-data **Task**."""
        pass

    @property
    def heartbeat_node_str(self) -> str:
        """:obj:`str`: Property with only getter. The node naming of meta-data **Heartbeat** path."""
        return "heartbeat"

    @property
    @abstractmethod
    def heartbeat(self) -> str:
        """:obj:`str`: Property with only getter. The path of meta-data **Heartbeat**."""
        pass


class MetaDataPath(BasePath):
    """*Base class to define all meta-data paths*

    All the path properties of meta-data objects.
    """

    @property
    def group_state(self) -> str:
        """:obj:`str`: Property with only getter. The path of meta-data **GroupState**."""
        return f"{self.generate_parent_node(self._group, is_group=True)}/{self.state_node_str}"

    @property
    def node_state(self) -> str:
        """:obj:`str`: Property with only getter. The path of meta-data **NodeState**."""
        return f"{self.generate_parent_node(self._name)}/{self.state_node_str}"

    @property
    def task(self) -> str:
        """:obj:`str`: Property with only getter. The path of meta-data **Task**."""
        return f"{self.generate_parent_node(self._name)}/{self.task_node_str}"

    @property
    def heartbeat(self) -> str:
        """:obj:`str`: Property with only getter. The path of meta-data **Heartbeat**."""
        return f"{self.generate_parent_node(self._name)}/{self.heartbeat_node_str}"

    @classmethod
    def generate_parent_node(cls, parent_name: str, is_group: bool = False) -> str:
        """Generate node path of Zookeeper with fixed format.

        Args:
            parent_name (str): The crawler name.
            is_group (bool): If it's True, generate node path for _group_ type meta-data.

        Returns:
            str: A Zookeeper node path.

        """
        if is_group:
            return f"group/{parent_name}"
        else:
            return f"node/{parent_name}"
