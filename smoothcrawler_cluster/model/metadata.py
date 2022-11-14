from datetime import datetime as dt
from typing import List, Union, Optional, TypeVar
from abc import ABCMeta, abstractmethod
import re

from .metadata_enum import CrawlerStateRole, TaskResult, HeartState

_DatetimeType = TypeVar("_DatetimeType", bound=dt)


class _BaseMetaData(metaclass=ABCMeta):

    def __str__(self):
        _dict_format_data = self.to_readable_object()
        return str(_dict_format_data)

    @abstractmethod
    def to_readable_object(self) -> dict:
        pass


class GroupState(_BaseMetaData):
    """
    One of the meta-data of *SmoothCrawler-Cluster*. It saves info about which VMs (web spider name) are **Runner**
    and another VMs are **Backup Runner**. The cluster would check the content of this info to run **Runner Election**
    to determine who is **Runner** and who is ***Backup Runner**.

    .. note::

        It could consider one thing: use 2 modes to determine how cluster works:

        1. *Decentralized mode*: no backup member, but each member could cover anyone which is dead.
        2. *Master-Slave mode*: has backup member and they do nothing (only check heartbeat) until anyone dead.

    * Zookeeper node path:

    /smoothcrawler/group/<crawler group name>/state/


    * Example info at node *state*:

    {
        "role": "runner",
        "total_crawler": 3,
        "total_runner": 2,
        "total_backup": 1,
        "current_crawler": ["spider_1", "spider_2", "spider_3"],
        "current_runner": ["spider_1", "spider_2"],
        "current_backup": ["spider_3"],
        "standby_id": "3",
        "fail_crawler": [],
        "fail_runner": [],
        "fail_backup": []
    }

    """

    _role: CrawlerStateRole = None

    _total_crawlers: int = None
    _total_runner: int = None
    _total_backup: int = None

    _current_crawler: List[str] = None
    _current_runner: List[str] = None
    _current_backup: List[str] = None

    _standby_id: str = None

    _fail_crawler: List[str] = None
    _fail_runner: List[str] = None
    _fail_backup: List[str] = None

    def to_readable_object(self) -> dict:
        _dict_format_data = {
            "role": self._role,
            "total_crawler": self.total_crawler,
            "total_runner": self.total_runner,
            "total_backup": self.total_backup,
            "standby_id": self.standby_id,
            "current_crawler": self.current_crawler,
            "current_runner": self.current_runner,
            "current_backup": self.current_backup,
            "fail_crawler": self.fail_crawler,
            "fail_runner": self.fail_runner,
            "fail_backup": self.fail_backup
        }
        return _dict_format_data

    @property
    def role(self) -> CrawlerStateRole:
        """
        Role of *SmoothCrawler-Cluster*. It recommends that use enum object **CrawlerStateRole** to configure this
        property. But it still could use string type value ('runner', 'backup-runner', 'dead-runner', 'dead-backup-runner')
        to do it.

        :return: The enumerate object *CrawlerStateRole* or string type value.
        """

        return self._role

    @role.setter
    def role(self, role: Union[CrawlerStateRole, str]) -> None:
        if type(role) is str:
            _enum_values = list(map(lambda a: a.value, CrawlerStateRole))
            if role not in _enum_values:
                raise ValueError("The value of attribute *role* is incorrect. It recommends that using enum object "
                                 f"*CrawlerStateRole*, but you also could use string which is valid if it in list {_enum_values}.")
        else:
            if type(role) is not CrawlerStateRole:
                raise ValueError(
                    "The value of attribute *role* is incorrect. Please use enum object *CrawlerStateRole*.")

        role = role.value if type(role) is CrawlerStateRole else role
        self._role = role

    @property
    def total_crawler(self) -> int:
        """
        The total count of the web spiders **Runner** & **Backup Runner**.

        :return: An integer value.
        """

        return self._total_crawlers

    @total_crawler.setter
    def total_crawler(self, total_crawler: int) -> None:
        if type(total_crawler) is not int:
            raise ValueError("Property *total_crawler* only accept int type value.")
        self._total_crawlers = total_crawler

    @property
    def total_runner(self) -> int:
        """
        The total count of web spider **Runner**.

        :return: An integer type value.
        """

        return self._total_runner

    @total_runner.setter
    def total_runner(self, total_runner: int) -> None:
        if type(total_runner) is not int:
            raise ValueError("Property *total_runner* only accept int type value.")
        self._total_runner = total_runner

    @property
    def total_backup(self) -> int:
        """
        The total count of web spider **Backup Runner**.

        :return: An integer value.
        """

        return self._total_backup

    @total_backup.setter
    def total_backup(self, total_backup: int) -> None:
        if type(total_backup) is not int:
            raise ValueError("Property *total_backup* only accept int type value.")
        self._total_backup = total_backup

    @property
    def current_crawler(self) -> List[str]:
        """
        The total count of **Runner** & **Backup Runner** currently.

        :return: A list type value which element is web spider name.
        """

        return self._current_crawler

    @current_crawler.setter
    def current_crawler(self, current_crawler: List[str]) -> None:
        if type(current_crawler) is not list:
            raise ValueError("Property *current_crawler* only accept list type value.")
        self._current_crawler = current_crawler

    @property
    def current_runner(self) -> List[str]:
        """
        The total count of **Runner** currently.

        :return: A list type value which element is web spider name.
        """

        return self._current_runner

    @current_runner.setter
    def current_runner(self, current_runner: List[str]) -> None:
        if type(current_runner) is not list:
            raise ValueError("Property *current_runner* only accept list type value.")
        self._current_runner = current_runner

    @property
    def current_backup(self) -> List[str]:
        """
        The total count of **Backup Runner** currently.

        :return: A list type value which element is web spider name.
        """

        return self._current_backup

    @current_backup.setter
    def current_backup(self, current_backup: List[str]) -> None:
        if type(current_backup) is not list:
            raise ValueError("Property *current_backup* only accept list type value.")
        self._current_backup = current_backup

    @property
    def standby_id(self) -> str:
        """
        The next one member of **Backup Runner** should activate to take over the task of **Runner** member which
        is dead (doesn't update heartbeat stamp and timeout).

        :return: An string type value and it is ID of the web spider's name.
        """

        return self._standby_id

    @standby_id.setter
    def standby_id(self, standby_id: str) -> None:
        if type(standby_id) is not str:
            raise ValueError("Property *standby_id* only accept str type value.")
        self._standby_id = standby_id

    @property
    def fail_crawler(self) -> List[str]:
        """
        The total count of **Dead Runner** & **Dead Backup Runner** currently.

        :return: A list type value which element is web spider name.
        """

        return self._fail_crawler

    @fail_crawler.setter
    def fail_crawler(self, fail_crawler: List[str]) -> None:
        if type(fail_crawler) is not list:
            raise ValueError("Property *fail_crawler* only accept list type value.")
        self._fail_crawler = fail_crawler

    @property
    def fail_runner(self) -> List[str]:
        """
        The total count of **Dead Runner** currently.

        :return: A list type value which element is web spider name.
        """

        return self._fail_runner

    @fail_runner.setter
    def fail_runner(self, fail_runner: List[str]) -> None:
        if type(fail_runner) is not list:
            raise ValueError("Property *fail_runner* only accept list type value.")
        self._fail_runner = fail_runner

    @property
    def fail_backup(self) -> List[str]:
        """
        The total count of **Dead Backup Runner** currently.

        :return: A list type value which element is web spider name.
        """

        return self._fail_backup

    @fail_backup.setter
    def fail_backup(self, fail_backup: List[str]) -> None:
        if type(fail_backup) is not list:
            raise ValueError("Property *fail_backup* only accept list type value.")
        self._fail_backup = fail_backup


class Task(_BaseMetaData):
    """
    The current web spider task **Runner** member got. It's the record for **Runner** or **Backup Runner** in different
    scenarios to do different things.

    **Runner** member
    -------------------

    For **Runner**, it could know which task and what detail it should use to run.


    **Backup Runner** member
    -------------------------

    For **Backup Runner**, it would try to get this info if anyone of **Runner** members doesn't update heartbeat stamp
    and timeout. And the **Backup Runner** member would use the info to know it should take over original **Runner**
    member to wait or run the current web spider task.


    * Zookeeper node path:

    /smoothcrawler/node/<crawler name>/task/


    * Example value at node *task*:

    {
        "task_content": {"url": "xxx", "method": "GET", "parameter": "", "body": ""},
        "task_result": "done"
    }

    """

    _task_content: dict = None
    _task_result: TaskResult = None

    def to_readable_object(self) -> dict:
        _dict_format_data = {
            "task_content": self.task_content,
            "task_result": self.task_result
        }
        return _dict_format_data

    @property
    def task_content(self) -> dict:
        """
        Detail of task. In generally, it should record some necessary data about task like
        *url*, *method*, etc.

        :return: A dict type value which saves task detail.
        """

        return self._task_content

    @task_content.setter
    def task_content(self, task_content: dict) -> None:
        if type(task_content) is not dict:
            raise ValueError("Property *task_content* only accept dict type value.")
        self._task_content = task_content

    @property
    def task_result(self) -> TaskResult:
        """
        The result of running wen spider task.

        :return: An enum object of **TaskResult**.
        """

        return self._task_result

    @task_result.setter
    def task_result(self, task_result: Union[TaskResult, str]) -> None:
        if type(task_result) is not str and type(task_result) is not TaskResult:
            raise ValueError("Property *task_result* only accept *str* or *TaskResult* type value.")
        task_result = task_result.value if type(task_result) is TaskResult else task_result
        self._task_result = task_result


class Heartbeat(_BaseMetaData):
    """
    The cluster member of **Backup Runner** would use this info to determine the member of **Runner** is health or not.
    It only has one thing in this section --- *datetime*. The *datetime* is the stamp of **Runner** heartbeat to display
    when does it live and update the stamp last time. So **Backup Runner** could keep checking this info to determine
    whether current **Runner** member is alive or not. And if the current **Runner** member doesn't update stamp until
    timeout and it also be discovered by **Backup Runner**, cluster rules that each **Backup Runner** members should
    check its index behind its web spider name and the smaller one should activate itself to run first, another ones which
    index are bigger should NOT activate and keep waiting / checking the heartbeat stamp until next time they discover
    timeout of heartbeat.

    * Zookeeper node path:

    /smoothcrawler/node/<crawler name>/heartbeat/


    * Example value at node *heartbeat*:

    {
        "heart_rhythm_time": "2022-07-15 08:42:59",
        "time_format": "%Y-%m-%d %H:%M:%S",
        "update_time": "2s",
        "update_timeout": "4s",
        "heart_rhythm_timeout": "3",
        "healthy_state": "Healthy",
        "task_state": "processing"
    }

    """

    _heart_rhythm_time: Union[str, dt] = None
    _time_format: str = None
    _update_time: str = None
    _update_timeout: str = None
    _heart_rhythm_timeout: str = None
    _healthy_state: str = None
    _task_state: str = None

    _Default_Time_Format: str = "%Y-%m-%d %H:%M:%S"

    _Time_Setting_Value_Format_Error = ValueError(
        "The value format of property *update_time* should be like '<number><timeunit>', i.e., '3s' or '2m', etc.")

    def to_readable_object(self) -> dict:
        _dict_format_data = {
            "heart_rhythm_time": self.heart_rhythm_time,
            "time_format": self.time_format,
            "update_time": self.update_time,
            "update_timeout": self.update_timeout,
            "heart_rhythm_timeout": self.heart_rhythm_timeout,
            "healthy_state": self.healthy_state,
            "task_state": self.task_state
        }
        return _dict_format_data

    @property
    def heart_rhythm_time(self) -> Optional[str]:
        """
        The datetime value of currently heartbeat of one specific **Runner** web spider.

        :return: A string type value.
        """

        if type(self._heart_rhythm_time) is str:
            return self._heart_rhythm_time
        else:
            if self._heart_rhythm_time is None:
                return self._heart_rhythm_time
            if self.time_format is not None:
                return self._heart_rhythm_time.strftime(self.time_format)
            return self._heart_rhythm_time.strftime(self._Default_Time_Format)

    @heart_rhythm_time.setter
    def heart_rhythm_time(self, heart_rhythm_time: Union[str, dt]) -> None:
        """
        Note:
            About the setter of property *heart_rhythm_time*, it could accept 2 types: _string_ or _datetime.datetime_.
            Because it wants to reach a feature about pre-checking the value it got is valid or not, it would try to parse
            the datetime value before set the value. However, if it's _string_ type value, it has so many format of datetime.
            So this property use a simple Python regex to check it:

            ``` python
                r"[0-9]{2,4}[\-\/:][0-9]{2,4}[\-\/:][0-9]{2,4}.[0-9]{2,4}[\-\/:][0-9]{2,4}[\-\/:][0-9]{2,4}"
            ```

            It try to check all format of datetime value, i.e., _yyyy-mm-dd hh:MM:DD_, _yy-mm-dd hh:MM:DD_, _dd-mm-yy hh:MM:DD_,
            etc, checking all values as possible. But, it would pre-check the datetime value easily if it already has property
            *time_format* value.
        """

        if type(heart_rhythm_time) is not str and type(heart_rhythm_time) is not dt:
            raise ValueError("Property *heart_rhythm_time* only accept 'str' type or 'datetime.datetime' value.")
        try:
            if type(heart_rhythm_time) is str:
                if self.time_format is not None and self.time_format != "":
                    dt.strptime(heart_rhythm_time, self.time_format)
                else:
                    _checksum = re.search(r"[0-9]{2,4}[\-\/:][0-9]{2,4}[\-\/:][0-9]{2,4}.[0-9]{2,4}[\-\/:][0-9]{2,4}"
                                          r"[\-\/:][0-9]{2,4}", str(heart_rhythm_time))
                    if _checksum.group() is None or _checksum.group() == "":
                        raise ValueError("The datetime type value is invalid. Please check it.")
            else:
                if self.time_format is not None and self.time_format != "":
                    heart_rhythm_time.strftime(self.time_format)
        except Exception as e:
            raise e
        else:
            self._heart_rhythm_time = heart_rhythm_time

    @property
    def time_format(self) -> str:
        """
        The string format of how smoothcrawler should to parse the value of *heart_rhythm_time*.

        :return: A string type value.
        """

        return self._time_format

    @time_format.setter
    def time_format(self, formatter: str) -> None:
        if type(formatter) is not str:
            raise ValueError("Property *time_format* only accept str type value.")
        _result = dt.now().strftime(formatter)
        if _result == formatter:
            raise ValueError("The value of property *time_format* is invalid. Please set a valid value as "
                             "'datetime.datetime.strftime()'.")
        self._time_format = formatter

    @property
    def update_time(self) -> str:
        """
        How long deos the crawler should keep updating the value of heartbeat property *heart_rhythm_time*. This property
        is the target to let smoothcrawler-cluster checks and may discover that one(s) of crawlers is(are) dead, please
        activating backup one(s) as soon as possible.

        :return: A string type value and its format would be like <int><string in (s,m,h)>, i.g., 3s, 1m. _s_ is seconds,
                 _m_ is minutes and _h_ is hours.
        """

        return self._update_time

    @update_time.setter
    def update_time(self, update_time: str) -> None:
        if type(update_time) is not str:
            raise ValueError("Property *update_time* only accept str type value.")
        if self._is_valid_times_value(update_time) is False:
            raise self._Time_Setting_Value_Format_Error

        self._update_time = update_time

    @property
    def update_timeout(self) -> str:
        """
        The timeout threshold to let others crawler judge current crawler instance is alive or dead. If it doesn't update
        the timestamp value of property *heart_rhythm_time* until the time is longer than this property's value, the crawler
        instance which doesn't update property *heart_rhythm_time* anymore would be marked as *HeartState.Arrhythmia* by others
        alive crawler instance.

        ..important:
        It does NOT mean that instance is dead. It just means it doesn't update the property *heart_rhythm_time* on time.

        :return: A string type value and its format would be like <int><string in (s,m,h)>, i.g., 3s, 1m. _s_ is seconds,
                 _m_ is minutes and _h_ is hours.
        """

        return self._update_timeout

    @update_timeout.setter
    def update_timeout(self, update_timeout: str) -> None:
        if type(update_timeout) is not str:
            raise ValueError("Property *update_timeout* only accept str type value.")
        if self._is_valid_times_value(update_timeout) is False:
            raise self._Time_Setting_Value_Format_Error

        self._update_timeout = update_timeout

    @property
    def heart_rhythm_timeout(self) -> str:
        """
        The property of *update_timeout* means one specific crawler instance doesn't update value on time, it's possible
        that the network issue lead to it happens, doesn't since the crawler instance is dead. And this property *heart_rhythm_timeout*
        means how many times is it late to update. It would truly be judged it is dead by others crawler instances if it
        reaches this threshold. And it would be marked as *HeartState.Asystole* by others crawler and be listed in *fail_crawler*.

        ..important:
        It truly means that instance is dead.

        :return: A string type value and must be _int_ type.
        """

        return self._heart_rhythm_timeout

    @heart_rhythm_timeout.setter
    def heart_rhythm_timeout(self, heart_rhythm_timeout: str) -> None:
        if type(heart_rhythm_timeout) is not str:
            raise ValueError("Property *heart_rhythm_time* only accept str type value.")
        try:
            int(heart_rhythm_timeout)
        except ValueError:
            raise ValueError(f"The value of property *heart_rhythm_time* should be ONLY number type value as 'int' "
                             f"type. But it got {heart_rhythm_timeout} currenctly.")
        else:
            self._heart_rhythm_timeout = heart_rhythm_timeout

    @property
    def healthy_state(self) -> str:
        """
        The healthy state of current crawler instance. It would be updated by itself or others in different scenarios.

        :return: A string type value which must be defined in HeartState enum object.
        """

        return self._healthy_state

    @healthy_state.setter
    def healthy_state(self, healthy_state: Union[str, HeartState]) -> None:
        if type(healthy_state) is not str and type(healthy_state) is not HeartState:
            raise ValueError("Property *heart_rhythm_time* only accept str type value.")
        if type(healthy_state) is str and healthy_state not in [i.value for i in HeartState]:
            raise ValueError("The value of property *healthy_state* should be as *HeartState* value.")
        healthy_state = healthy_state.value if type(healthy_state) is HeartState else healthy_state
        self._healthy_state = healthy_state

    @property
    def task_state(self) -> str:
        """
        The running state of the task it takes currently.

        :return: A string type value which must be defined in TaskResult enum object.
        """

        return self._task_state

    @task_state.setter
    def task_state(self, task_state: Union[str, TaskResult]) -> None:
        if type(task_state) is not str and type(task_state) is not TaskResult:
            raise ValueError("Property *task_state* only accept *str* or *TaskResult* type value.")
        if type(task_state) is str and task_state not in [i.value for i in TaskResult]:
            raise ValueError("The value of property *task_state* should be as *TaskResult* value.")
        task_state = task_state.value if type(task_state) is TaskResult else task_state
        self._task_state = task_state

    def _is_valid_times_value(self, times: str) -> bool:
        _number_and_timeunit = re.search(r"[0-9]{1,16}[smh]", times)
        return True if _number_and_timeunit.group() else False
