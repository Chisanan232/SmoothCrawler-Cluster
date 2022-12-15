"""Module
TODO: Add docstring
"""

from enum import Enum


class CrawlerStateRole(Enum):
    """
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

    Initial = "initial"
    """
    It only have this state in instantiating process of crawler and before runner election. And it would change this 
    option to *Runner* or *Backup_Runner* after done runner election.
    """

    Runner = "runner"
    """ Literally, **Runner** role is the major element to run web spider tasks. """

    Backup_Runner = "backup-runner"
    """
    **Backup Runner** role is the backup of **Runner**. It would activate (base on the setting, it may activate 
    immediately) and run the web spider task if it still not finish. 
    A **Backup Runner** would keep checking the heartbeat info of **Runner**, standby and ready to run in anytime 
    for any one of **Runner** does not keep updating its own heartbeat info (and it would turn to **Dead Runner** 
    at that time).
    """

    Dead_Runner = "dead-runner"
    """
    If **Runner** cannot work finely, like the entire VM be shutdown where the crawler runtime environment in. It 
    would turn to be **Dead Runner** from **Runner**. In other words, it must to be **Dead Runner** if it cannot 
    keep updating its own heartbeat info.
    """

    Dead_Backup_Runner = "dead-backup-runner"
    """ **Dead Backup Runner** is same as **Dead Runner** but it's for **Backup Runner**. """


class TaskResult(Enum):
    """
    The task result means it is the result of running web spider task. The web spider task could classify to be
    4 different states: processing, done, terminate and error.
    """

    Nothing = "nothing"
    """ When crawler just be ready for running or in instantiating process, It would be this state. """

    Processing = "processing"
    """ Task running in processing. """

    Done = "done"
    """ Finish the task and it works finely without any exceptions. """

    Terminate = "terminate"
    """
    Web spider task running has been terminated so that it cannot finish all processes, but it doesn't occur any 
    exceptions except KeyboardInterrupt.
    """

    Error = "error"
    """ If it raise any exceptions in web spider task running, its result would be error. """


class HeartState(Enum):
    """
    The heartbeat state.
    """

    Newborn = "Newborn"
    """
    The initialized heartbeat state of a crawler instance which just be created, in other words, crawler instance's 
    heartbeat state would be this before it run updating heartbeat process. 
    """

    Healthy = "Healthy"
    """ This means that current crawler instance runs finely without any issue. """

    Arrhythmia = "Arrhythmia"
    """
    It means that the current crawler instance is late to update property *heart_rhythm_time*. It likes its heartbeat 
    rate is not stable and cannot work normally lead to it looks or sounds not good, just like arrhythmia in human.
    """

    Asystole = "Asystole"
    """
    If a crawler instance's property *healthy_state* become this state, it means the crawler be judged it's dead by 
    other alive crawler instances. So the dead crawler instance won't run anymore and list to *fail_xxx* property in 
    *State*.
    """

    ApparentDeath = "Apparent Death"
    """ It would be this state if the crawler instance would be stopped or killed manually. """
