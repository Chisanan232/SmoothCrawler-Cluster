"""Module Docstring
TODO: Add docstring
"""

from abc import ABCMeta, abstractmethod
from collections import namedtuple
from datetime import datetime as dt
from typing import List, Union, Optional, TypeVar
import re

from .metadata_enum import CrawlerStateRole, TaskResult, HeartState

_DatetimeType = TypeVar("_DatetimeType", bound=dt)


class _BaseMetaData(metaclass=ABCMeta):

    def __str__(self):
        return str(self.to_readable_object())

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
        return {
            "total_crawler": self.total_crawler,
            "total_runner": self.total_runner,
            "total_backup": self.total_backup,
            "standby_id": self.standby_id,
            "current_crawler": self.current_crawler,
            "current_runner": self.current_runner,
            "current_backup": self.current_backup,
            "fail_crawler": self.fail_crawler,
            "fail_runner": self.fail_runner,
            "fail_backup": self.fail_backup,
        }

    @property
    def total_crawler(self) -> int:
        """
        The total count of the web spiders **Runner** & **Backup Runner**.

        :return: An integer value.
        """

        return self._total_crawlers

    @total_crawler.setter
    def total_crawler(self, total_crawler: int) -> None:
        if isinstance(total_crawler, int) is False:
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
        if isinstance(total_runner, int) is False:
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
        if isinstance(total_backup, int) is False:
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
        if isinstance(current_crawler, list) is False:
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
        if isinstance(current_runner, list) is False:
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
        if isinstance(current_backup, list) is False:
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
        if isinstance(standby_id, str) is False:
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
        if isinstance(fail_crawler, list) is False:
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
        if isinstance(fail_runner, list) is False:
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
        if isinstance(fail_backup, list) is False:
            raise ValueError("Property *fail_backup* only accept list type value.")
        self._fail_backup = fail_backup


class NodeState(_BaseMetaData):
    """
    _GroupState_ is a state info for multiple nodes which also in same **group**. _NodeState_ is the state info for one
    specific crawler instance. It would save some more detail info of the crawler instance of the cluster.

    * Zookeeper node path:

    /smoothcrawler/node/<crawler name>/state/


    * Example info at node *state*:

    {
        "role": "runner",
        "group": "sc-crawler-cluster"
    }

    """

    _group: str = None
    _role: CrawlerStateRole = None

    def to_readable_object(self) -> dict:
        return {
            "group": self.group,
            "role": self._role,
        }

    @property
    def group(self) -> str:
        """
        The name of group current crawler instance in.

        :return: group name.
        """
        return self._group

    @group.setter
    def group(self, group: str) -> None:
        if isinstance(group, str) is False:
            raise ValueError("Property *group* only accept 'str' type value.")
        self._group = group

    @property
    def role(self) -> CrawlerStateRole:
        """
        Role of *SmoothCrawler-Cluster*. It recommends that use enum object **CrawlerStateRole** to configure this
        property. But it still could use string type value ('runner', 'backup-runner', 'dead-runner', 'dead-backup-
        runner') to do it.

        :return: The enumerate object *CrawlerStateRole* or string type value.
        """

        return self._role

    @role.setter
    def role(self, role: Union[CrawlerStateRole, str]) -> None:
        if isinstance(role, str):
            enum_values = list(map(lambda a: a.value, CrawlerStateRole))
            if role not in enum_values:
                raise ValueError("The value of attribute *role* is incorrect. It recommends that using enum object "
                                 f"*CrawlerStateRole*, but you also could use string which is valid if it in list "
                                 f"{enum_values}.")
        else:
            if isinstance(role, CrawlerStateRole) is False:
                raise ValueError(
                    "The value of attribute *role* is incorrect. Please use enum object *CrawlerStateRole*.")

        role = role.value if isinstance(role, CrawlerStateRole) else role
        self._role = role


_RunningContent_Attrs: List[str] = ["task_id", "url", "method", "parameters", "header", "body"]
RunningContent = namedtuple("RunningContent", _RunningContent_Attrs)

_RunningResult_Attrs: List[str] = ["success_count", "fail_count"]
RunningResult = namedtuple("RunningResult", _RunningResult_Attrs)

_ResultDetail_Attrs: List[str] = ["task_id", "state", "status_code", "response", "error_msg"]
ResultDetail = namedtuple("ResultDetail", _ResultDetail_Attrs)


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
        "running_content": [
            {
                "task_id": 0,
                "url": "https://www.example.com",
                "method": "GET",
                "parameters": None,
                "header": None,
                "body": None
            }
        ],
        "cookies": "{}",
        "authorization": {},
        "in_progressing_id": "0",
        "running_result": {
            "success_count": 0,
            "fail_count": 0
        },
        "running_status": "running",
        "result_detail": [
            {
                "task_id": 0,
                "state": "done",
                "status_code": 200,
                "response": "",
                "error_msg": None
            }
        ],
    }

    """

    _running_content: List[dict] = None
    _cookie: dict = None
    _authorization: dict = None
    _in_progressing_id: str = None
    _running_result: dict = None
    _running_status: str = None
    _result_detail: List[dict] = None

    def to_readable_object(self) -> dict:
        return {
            "running_content": self._running_content,
            "cookie": self._cookie,
            "authorization": self._authorization,
            "in_progressing_id": self._in_progressing_id,
            "running_result": self._running_result,
            "running_status": self._running_status,
            "result_detail": self._result_detail,
        }

    @property
    def running_content(self) -> List[dict]:
        """
        Detail of task. In generally, it should record some necessary data about task like *url*, *method*, etc. It
        suggests developers use object *RunningContent* to configure this attribute. This would be reset after crawler
        instance finish all tasks.

        :return: A list type value which be combined with dict type elements.
        """

        return self._running_content

    @running_content.setter
    def running_content(self, running_content: List[Union[dict, RunningContent]]) -> None:
        if isinstance(running_content, list) is False:
            raise ValueError("Property *running_content* only accept list type object which be combined with "
                             "RunningContent type elements.")
        chksum = map(lambda content: isinstance(content, (dict, RunningContent)), running_content)
        if False in list(chksum):
            raise ValueError("Property *running_content* only accept list with RunningContent type elements.")
        dict_contents = map(
            lambda content: self.__to_dict(content, _RunningContent_Attrs)
            if isinstance(content, RunningContent) else content,
            running_content)
        self._running_content = list(dict_contents)

    @property
    def cookie(self) -> dict:
        """
        The cookie for tasks.

        :return: A dict type value.
        """

        return self._cookie

    @cookie.setter
    def cookie(self, cookie: dict) -> None:
        if isinstance(cookie, dict) is False:
            raise ValueError("Property *cookie* only accept dict type value.")
        self._cookie = cookie

    @property
    def authorization(self) -> dict:
        """
        The authorization for tasks.

        :return: A dict type value.
        """

        return self._authorization

    @authorization.setter
    def authorization(self, authorization: dict) -> None:
        if isinstance(authorization, dict) is False:
            raise ValueError("Property *authorization* only accept dict type value.")
        self._authorization = authorization

    @property
    def in_progressing_id(self) -> str:
        """
        The ID of the task which be run by crawler instance currently.

        :return: A dict type value.
        """

        return self._in_progressing_id

    @in_progressing_id.setter
    def in_progressing_id(self, in_progressing_id: Union[str, int]) -> None:
        if isinstance(in_progressing_id, (str, int)) is False:
            raise ValueError("Property *in_progressing_id* only accept int type value or str type value which is a "
                             "number format.")
        try:
            int(in_progressing_id)
        except ValueError as e:
            raise ValueError("Property *in_progressing_id* only accept str type value which is a number format.") from e
        else:
            self._in_progressing_id = str(in_progressing_id)

    @property
    def running_result(self) -> dict:
        """
        This attribute records statistics of the running result of tasks. Currently, it only has 2 type data: count of
        success (_success_count_) and count of fail (_fail_count_).

        :return: A dict type value.
        """

        return self._running_result

    @running_result.setter
    def running_result(self, running_result: Union[dict, RunningResult]) -> None:
        if isinstance(running_result, (dict, RunningResult)) is False:
            raise ValueError("Property *running_result* only accept dict type or RunningResult type value.")
        running_result = self.__to_dict(running_result, _RunningResult_Attrs) \
            if isinstance(running_result, RunningResult) else running_result
        self._running_result = running_result

    @property
    def running_status(self) -> str:
        """
        The status of crawler runs task. It suggests developers configure this attribute by enum object **TaskResult**.

        :return: A string type value from enum object of **TaskResult**.
        """

        return self._running_status

    @running_status.setter
    def running_status(self, running_status: Union[str, TaskResult]) -> None:
        if isinstance(running_status, (str, TaskResult)) is False:
            raise ValueError("Property *running_status* only accept *str* or *TaskResult* type value.")
        result_detail = running_status.value if isinstance(running_status, TaskResult) else running_status
        self._running_status = result_detail

    @property
    def result_detail(self) -> List[dict]:
        """
        This attributes saves the details of tasks running result.

        :return: A list type valur which be combined with dict type data be defined by object **ResultDetail**.
        """

        return self._result_detail

    @result_detail.setter
    def result_detail(self, result_detail: List[Union[dict, ResultDetail]]) -> None:
        if isinstance(result_detail, list) is False:
            raise ValueError("Property *result_detail* only accept list type object which be combined with ResultDetail"
                             " type elements.")
        chksum = map(lambda detail: isinstance(detail, (dict, ResultDetail)), result_detail)
        if False in list(chksum):
            raise ValueError("Property *result_detail* only accept list with ResultDetail type elements.")
        dict_details = map(
            lambda detail: self.__to_dict(detail, _ResultDetail_Attrs) if isinstance(detail, ResultDetail) else detail,
            result_detail)
        self._result_detail = list(dict_details)

    @classmethod
    def __to_dict(cls, obj, attrs: List[str]) -> dict:
        value = {}
        for attr in attrs:
            value[attr] = getattr(obj, attr)
        return value


class Heartbeat(_BaseMetaData):
    """
    The cluster member of **Backup Runner** would use this info to determine the member of **Runner** is health or not.
    It only has one thing in this section --- *datetime*. The *datetime* is the stamp of **Runner** heartbeat to display
    when does it live and update the stamp last time. So **Backup Runner** could keep checking this info to determine
    whether current **Runner** member is alive or not. And if the current **Runner** member doesn't update stamp until
    timeout and it also be discovered by **Backup Runner**, cluster rules that each **Backup Runner** members should
    check its index behind its web spider name and the smaller one should activate itself to run first, another ones
    which index are bigger should NOT activate and keep waiting / checking the heartbeat stamp until next time they
    discover timeout of heartbeat.

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

    _default_time_format: str = "%Y-%m-%d %H:%M:%S"

    _Time_Setting_Value_Format_Error = ValueError(
        "The value format of property *update_time* should be like '<number><timeunit>', i.e., '3s' or '2m', etc.")

    def to_readable_object(self) -> dict:
        return {
            "heart_rhythm_time": self.heart_rhythm_time,
            "time_format": self.time_format,
            "update_time": self.update_time,
            "update_timeout": self.update_timeout,
            "heart_rhythm_timeout": self.heart_rhythm_timeout,
            "healthy_state": self.healthy_state,
            "task_state": self.task_state,
        }

    @property
    def heart_rhythm_time(self) -> Optional[str]:
        """
        The datetime value of currently heartbeat of one specific **Runner** web spider.

        :return: A string type value.
        """

        if isinstance(self._heart_rhythm_time, str):
            return self._heart_rhythm_time
        else:
            if self._heart_rhythm_time is None:
                return self._heart_rhythm_time
            if self.time_format:
                return self._heart_rhythm_time.strftime(self.time_format)
            return self._heart_rhythm_time.strftime(self._default_time_format)

    @heart_rhythm_time.setter
    def heart_rhythm_time(self, heart_rhythm_time: Union[str, dt]) -> None:
        """
        Note:
            About the setter of property *heart_rhythm_time*, it could accept 2 types: _string_ or _datetime.datetime_.
            Because it wants to reach a feature about pre-checking the value it got is valid or not, it would try to
            parse the datetime value before set the value. However, if it's _string_ type value, it has so many format
            of datetime. So this property use a simple Python regex to check it:

            ``` python
                r"[0-9]{2,4}[\-\/:][0-9]{2,4}[\-\/:][0-9]{2,4}.[0-9]{2,4}[\-\/:][0-9]{2,4}[\-\/:][0-9]{2,4}"
            ```

            It try to check all format of datetime value, i.e., _yyyy-mm-dd hh:MM:DD_, _yy-mm-dd hh:MM:DD_,
            _dd-mm-yy hh:MM:DD_, etc, checking all values as possible. But, it would pre-check the datetime value easily
            if it already has property *time_format* value.
        """

        if isinstance(heart_rhythm_time, (str, dt)) is False:
            raise ValueError("Property *heart_rhythm_time* only accept 'str' type or 'datetime.datetime' value.")
        if isinstance(heart_rhythm_time, str):
            if self.time_format:
                dt.strptime(heart_rhythm_time, self.time_format)
            else:
                checksum = re.search(
                    r"[0-9]{2,4}[\-\/:][0-9]{2,4}[\-\/:][0-9]{2,4}.[0-9]{2,4}[\-\/:][0-9]{2,4}[\-\/:][0-9]{2,4}",
                    str(heart_rhythm_time))
                if checksum.group() is None or checksum.group() == "":
                    raise ValueError("The datetime type value is invalid. Please check it.")
        else:
            if self.time_format:
                heart_rhythm_time.strftime(self.time_format)
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
        if isinstance(formatter, str) is False:
            raise ValueError("Property *time_format* only accept str type value.")
        result = dt.now().strftime(formatter)
        if result == formatter:
            raise ValueError("The value of property *time_format* is invalid. Please set a valid value as "
                             "'datetime.datetime.strftime()'.")
        self._time_format = formatter

    @property
    def update_time(self) -> str:
        """
        How long deos the crawler should keep updating the value of heartbeat property *heart_rhythm_time*. This
        property is the target to let smoothcrawler-cluster checks and may discover that one(s) of crawlers is(are)
        dead, please activating backup one(s) as soon as possible.

        :return: A string type value and its format would be like <int><string in (s,m,h)>, i.g., 3s, 1m. _s_ is
        seconds, _m_ is minutes and _h_ is hours.
        """

        return self._update_time

    @update_time.setter
    def update_time(self, update_time: str) -> None:
        if isinstance(update_time, str) is False:
            raise ValueError("Property *update_time* only accept str type value.")
        if self._is_valid_times_value(update_time) is False:
            raise self._Time_Setting_Value_Format_Error

        self._update_time = update_time

    @property
    def update_timeout(self) -> str:
        """
        The timeout threshold to let others crawler judge current crawler instance is alive or dead. If it doesn't
        update the timestamp value of property *heart_rhythm_time* until the time is longer than this property's value,
        the crawler instance which doesn't update property *heart_rhythm_time* anymore would be marked as
        *HeartState.Arrhythmia* by others alive crawler instance.

        ..important:
        It does NOT mean that instance is dead. It just means it doesn't update the property *heart_rhythm_time* on
        time.

        :return: A string type value and its format would be like <int><string in (s,m,h)>, i.g., 3s, 1m. _s_ is
                 seconds, _m_ is minutes and _h_ is hours.
        """

        return self._update_timeout

    @update_timeout.setter
    def update_timeout(self, update_timeout: str) -> None:
        if isinstance(update_timeout, str) is False:
            raise ValueError("Property *update_timeout* only accept str type value.")
        if self._is_valid_times_value(update_timeout) is False:
            raise self._Time_Setting_Value_Format_Error

        self._update_timeout = update_timeout

    @property
    def heart_rhythm_timeout(self) -> str:
        """
        The property of *update_timeout* means one specific crawler instance doesn't update value on time, it's
        possible that the network issue lead to it happens, doesn't since the crawler instance is dead. And this
        property *heart_rhythm_timeout* means how many times is it late to update. It would truly be judged it is
        dead by others crawler instances if it reaches this threshold. And it would be marked as *HeartState.Asystole*
        by others crawler and be listed in *fail_crawler*.

        ..important:
        It truly means that instance is dead.

        :return: A string type value and must be _int_ type.
        """

        return self._heart_rhythm_timeout

    @heart_rhythm_timeout.setter
    def heart_rhythm_timeout(self, heart_rhythm_timeout: str) -> None:
        if isinstance(heart_rhythm_timeout, str) is False:
            raise ValueError("Property *heart_rhythm_time* only accept str type value.")
        try:
            int(heart_rhythm_timeout)
        except ValueError as e:
            raise ValueError(f"The value of property *heart_rhythm_time* should be ONLY number type value as 'int' type"
                             f". But it got {heart_rhythm_timeout} currently.") from e
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
        if isinstance(healthy_state, (str, HeartState)) is False:
            raise ValueError("Property *heart_rhythm_time* only accept str type value.")
        if isinstance(healthy_state, str) and healthy_state not in [i.value for i in HeartState]:
            raise ValueError("The value of property *healthy_state* should be as *HeartState* value.")
        healthy_state = healthy_state.value if isinstance(healthy_state, HeartState) else healthy_state
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
        if isinstance(task_state, (str, TaskResult)) is False:
            raise ValueError("Property *task_state* only accept *str* or *TaskResult* type value.")
        if isinstance(task_state, str) and task_state not in [i.value for i in TaskResult]:
            raise ValueError("The value of property *task_state* should be as *TaskResult* value.")
        task_state = task_state.value if isinstance(task_state, TaskResult) else task_state
        self._task_state = task_state

    @classmethod
    def _is_valid_times_value(cls, times: str) -> bool:
        number_and_timeunit = re.search(r"[0-9]{1,16}(s|m|h)", times)
        return number_and_timeunit is not None
