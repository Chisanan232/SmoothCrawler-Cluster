from typing import List, AnyStr, Union
from enum import Enum



class CrawlerStateRole(Enum):
    """
    This role is NOT the role of *SmoothCrawler-AppIntegration*. They're very different. The role in
    *SmoothCrawler-AppIntegration* means source site (or producer) of application or processor site
    (or consumer) of application. But the role in meta-data in *SmoothCrawler-Cluster* means it is
    active runner to run task or backup of that active runner.

    For **SmoothCrawler-Cluster** realm, it has 4 different roles:

    * Runner
    * Backup Runner
    * Dead Runner
    * Dead Backup Runner
    """

    Runner = "runner"
    """ Literally, **Runner** role is the major element to run web spider tasks. """

    Backup_Runner = "backup-runner"
    """
    **Backup Runner** role is the backup of **Runner**. It would activate (base on the setting, it may 
    activate immediately) and run the web spider task if it still not finish. 
    A **Backup Runner** would keep checking the heartbeat info of **Runner**, standby and ready to run 
    in anytime for any one of **Runner** does not keep updating its own heartbeat info (and it would turn 
    to **Dead Runner** at that time).
    """

    Dead_Runner = "dead-runner"
    """ 
    If **Runner** cannot work finely, like the entire VM be shutdown where the crawler runtime environment 
    in. It would turn to be **Dead Runner** from **Runner**. In other words, it must to be **Dead Runner** 
    if it cannot keep updating its own heartbeat info.
    """

    Dead_Backup_Runner = "dead-backup-runner"
    """ **Dead Backup Runner** is same as **Dead Runner** but it's for **Backup Runner**. """



class TaskResult(Enum):
    """
    The task result means it is the result of running web spider task. The web spider task could classify to
    be 4 different states: processing, done, terminate and error.
    """

    Processing = "processing"
    """ Task running in processing. """

    Done = "done"
    """ Finish the task and it works finely without any exceptions. """

    Terminate = "terminate"
    """ 
    Web spider task running has been terminated so that it cannot finish all processes, but it doesn't occur 
    any exceptions except KeyboardInterrupt.
    """

    Error = "error"
    """ If it raise any exceptions in web spider task running, its result would be error. """



class State:
    """
    One of the meta-data of *SmoothCrawler-Cluster*. It saves info about which VMs (web spider name) are **Runner**
    and another VMs are **Backup Runner**. The cluster would check the content of this info to run **Runner Election**
    to determine who is **Runner** and who is ***Backup Runner**.

    .. note::

        It could consider one thing: use 2 modes to determine how cluster works:

        1. *Decentralized mode*: no backup member, but each member could cover anyone which is dead.
        2. *Master-Slave mode*: has backup member and they do nothing (only check heartbeat) until anyone dead.

    * Zookeeper node path:

    /smoothcrawler/node/<crawler name>/state/


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
                raise ValueError("The value of attribute *role* is incorrect. Please use enum object *CrawlerStateRole*.")

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



class Task:
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
            raise ValueError("Property *task_content* only accept list type value.")
        self._task_content = task_content


    @property
    def task_result(self) -> TaskResult:
        """
        The result of running wen spider task.

        :return: An enum object of **TaskResult**.
        """

        return self._task_result


    @task_result.setter
    def task_result(self, task_result: TaskResult) -> None:
        if type(task_result) is not TaskResult:
            raise ValueError("Property *task_result* only accept *TaskResult* type value.")
        self._task_result = task_result



class Heartbeat:
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
        "datetime": "2022:07:15 08:42:59"
    }

    """

    _datetime: str = None

    @property
    def datetime(self) -> str:
        """
        The datetime value of currently heartbeat of one specific **Runner** web spider.

        :return: A string type value.
        """

        return self._datetime


    @datetime.setter
    def datetime(self, datetime: str) -> None:
        self._datetime = datetime
