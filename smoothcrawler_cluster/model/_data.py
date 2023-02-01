"""*Data for inner usage*

Here provides some data objects for inner usage in *SmoothCrawler-Cluster*.
"""

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
