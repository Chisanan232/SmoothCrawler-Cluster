"""*Enum of meta-data which has multiple fixed values*

For some meta-data options, e.g., *role* of **NodeState**, it has multiple fixed values as signal at it, and it won't
let outside modify it or assign it to meta-data casually. The module provides some enum objects to usage of meta-data
to let value could be more clear and expected.
"""

from enum import Enum


class CrawlerStateRole(Enum):
    """*The crawler role in crawler cluster system*

    This role is NOT the role of *SmoothCrawler-AppIntegration*. They're very different. The role in
    *SmoothCrawler-AppIntegration* means source site (or producer) of application or processor site
    (or consumer) of application. But the role in meta-data in *SmoothCrawler-Cluster* means it is
    active runner to run task or backup of that active runner.

    For **SmoothCrawler-Cluster** realm, it has 4 different roles:

    * Initial
    * Runner
    * Backup Runner
    * Dead Runner
    * Dead Backup Runner
    """

    INITIAL = "initial"
    """It only have this state in instantiating process of crawler and before runner election. And it would change this
    option to *Runner* or *Backup_Runner* after done runner election.
    """

    RUNNER = "runner"
    """Literally, **Runner** role is the major element to run web spider tasks."""

    BACKUP_RUNNER = "backup-runner"
    """**Backup Runner** role is the backup of **Runner**. It would activate (base on the setting, it may activate
    immediately) and run the web spider task if it still not finish.

    A **Backup Runner** would keep checking the heartbeat info of **Runner**, standby and ready to run in anytime
    for any one of **Runner** does not keep updating its own heartbeat info (and it would turn to **Dead Runner**
    at that time).
    """

    DEAD_RUNNER = "dead-runner"
    """If **Runner** cannot work finely, like the entire VM be shutdown where the crawler runtime environment in. It
    would turn to be **Dead Runner** from **Runner**. In other words, it must to be **Dead Runner** if it cannot keep
    updating its own heartbeat info.
    """

    DEAD_BACKUP_RUNNER = "dead-backup-runner"
    """**Dead Backup Runner** is same as **Dead Runner** but it's for **Backup Runner**."""


class TaskResult(Enum):
    """*The task running state*

    The task result means it is the result of running web spider task. The web spider task could classify to be
    4 different states: processing, done, terminate and error.
    """

    NOTHING = "nothing"
    """When crawler just be ready for running or in instantiating process, It would be this state."""

    PROCESSING = "processing"
    """Task running in processing."""

    DONE = "done"
    """Finish the task and it works finely without any exceptions."""

    TERMINATE = "terminate"
    """Web spider task running has been terminated so that it cannot finish all processes, but it doesn't occur any
    exceptions except KeyboardInterrupt.
    """

    ERROR = "error"
    """If it raise any exceptions in web spider task running, its result would be error."""


class HeartState(Enum):
    """*State of heartbeat at that time*

    The heartbeat state.
    """

    NEWBORN = "Newborn"
    """The initialized heartbeat state of a crawler instance which just be created, in other words, crawler instance's
    heartbeat state would be this before it run updating heartbeat process.
    """

    HEALTHY = "Healthy"
    """This means that current crawler instance runs finely without any issue."""

    ARRHYTHMIA = "Arrhythmia"
    """It means that the current crawler instance is late to update property *heart_rhythm_time*. It likes its heartbeat
    rate is not stable and cannot work normally lead to it looks or sounds not good, just like arrhythmia in human.
    """

    ASYSTOLE = "Asystole"
    """If a crawler instance's property *healthy_state* become this state, it means the crawler be judged it's dead by
    other alive crawler instances. So the dead crawler instance won't run anymore and list to *fail_xxx* property in
    *State*.
    """

    APPARENT_DEATH = "Apparent Death"
    """It would be this state if the crawler instance would be stopped or killed manually."""
