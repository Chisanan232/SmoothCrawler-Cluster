"""*The different independent processing for different role as the role's own workflow*

*SmoothCrawler-Cluster* let the implementation details of each role's processing as each role's individual workflow. And
the workflow would be dispatched to do the process which it should work at that time in crawler by the dispatcher in
*dispatcher* module. So it won't take care who use them, it only considers that what things it should do in the cluster
by the role.
"""

import re
import time
from abc import ABCMeta, abstractmethod
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Type

from .._utils import parse_timer
from .._utils.converter import TaskContentDataUtils
from ..exceptions import StopUpdateHeartbeat
from ..model import (
    CrawlerStateRole,
    GroupState,
    Heartbeat,
    HeartState,
    NodeState,
    ResultDetail,
    RunningResult,
    Task,
    TaskResult,
    Update,
)
from ..model._data import CrawlerName, CrawlerTimer, MetaDataOpt, MetaDataPath
from .adapter import DistributedLock


class BaseWorkflow(metaclass=ABCMeta):
    """*The base running workflow*

    It rules 2 things: workflow object initial arguments and major function *run*.

    * Initial arguments
        Workflow is responsible for each role's processing in cluster. So it must need the basic crawler info: *name*
        and *index separation*. And it also needs to communicate with each other to know the cluster brief state, so it
        also needs *path of meta-data* and *callback function for getting and setting meta-data*.

    * Major function *run*
        The main body of what details the workflow would do. So no matter that workflows be called or dispatched, caller
        could only use `<BaseWorkflow object>.run(*args, **kwargs)` to run it.
    """

    def __init__(
        self,
        name: CrawlerName,
        path: Type[MetaDataPath],
        metadata_opts_callback: MetaDataOpt,
    ):
        """

        Args:
            name (CrawlerName): The data object **CrawlerName** which provides some attribute like crawler instance's
                name or ID, etc.
            path (Type[MetaDataPath]): The objects which has all meta-data object's path property.
            metadata_opts_callback (MetaDataOpt): The data object *MetaDataOpt* which provides multiple callback
                functions about getting and setting meta-data.
        """
        self._crawler_name = name
        self._path = path
        # get_metadata (Callable): The callback function about getting meta-data as object.
        self._get_metadata = metadata_opts_callback.get_callback
        # set_metadata (Callable): The callback function about setting meta-data from object.
        self._set_metadata = metadata_opts_callback.set_callback

    @abstractmethod
    def run(self, *args, **kwargs) -> Any:
        """
        The major function for running this workflow. This abstract function's argument use ``*args`` and ``**kwargs``
        because it let all workflow could have their own needed customized arguments, absolutely it also could have
        nothing arguments if it needs.

        Args:
            *args (tuple): Function arguments which would be used as **args*.
            **kwargs (dict): Function arguments which would be used as ***kwargs*.

        Returns:

        """
        pass


class BaseRoleWorkflow(BaseWorkflow):
    """*Base of each role's workflow*

    The base class of each role's workflow which should have property of what *role* it is for. And the major function
    *run* must needs to pass argument *timer*.
    """

    def __init__(
        self,
        name: CrawlerName,
        path: Type[MetaDataPath],
        metadata_opts_callback: MetaDataOpt,
        lock: DistributedLock,
        crawler_process_callback: Callable,
    ):
        """

        Args:
            name (CrawlerName): The data object **CrawlerName** which provides some attribute like crawler instance's
                name or ID, etc.
            path (Type[MetaDataPath]): The objects which has all meta-data object's path property.
            metadata_opts_callback (MetaDataOpt): The data object *MetaDataOpt* which provides multiple callback
                functions about getting and setting meta-data.
            lock (DistributedLock): The adapter of distributed lock.
            crawler_process_callback (Callable): The callback function about running the crawler core processes.
        """
        super().__init__(
            name=name,
            path=path,
            metadata_opts_callback=metadata_opts_callback,
        )
        self._lock = lock
        self._crawler_process_callback = crawler_process_callback

    @property
    @abstractmethod
    def role(self) -> CrawlerStateRole:
        """:obj:`str`: Property with only getter for the crawler role what current workflow for."""
        pass

    @abstractmethod
    def run(self, timer: CrawlerTimer) -> Any:
        """Run the processes of the role's workflow.

        Args:
            timer (CrawlerTimer): The object which has some timer attributes.

        Returns:
            No rule. It could return anything it needs in each role's workflow.

        """
        pass


class RunnerWorkflow(BaseRoleWorkflow):
    """*The processing of role **RUNNER***

    The crawler role **RUNNER** needs to run tasks so that it also needs to monitor the meta-data object **Task** to
    check whether it has tasks or not and runs it if it has.
    """

    @property
    def role(self) -> CrawlerStateRole:
        return CrawlerStateRole.RUNNER

    def run(self, timer: CrawlerTimer) -> None:
        """Keep waiting for tasks coming and run it.

        Args:
            timer (CrawlerTimer): The object which has some timer attributes. In this workflow, it would only use one
                attribute --- *CrawlerTimer.time_interval.check_task* (float), a property about the frequency of
                checking task.

                * *CrawlerTimer.time_interval.check_task* (`float`_)
                    how long does the crawler instance wait a second for next task. The unit is seconds and default
                    value is 2.

        Returns:
            None

        .. _float: https://docs.python.org/3/library/functions.html#float
        """
        # 1. Try to get data from the target mode path of Zookeeper
        # 2. if (step 1 has data and it's valid) {
        #        Start to run tasks from the data
        #    } else {
        #        Keep wait for tasks
        #    }
        while True:
            node_state = self._get_metadata(path=self._path.node_state, as_obj=NodeState, must_has_data=False)
            task = self._get_metadata(path=self._path.task, as_obj=Task, must_has_data=False)

            if node_state.role == CrawlerStateRole.DEAD_RUNNER.value:
                raise StopUpdateHeartbeat

            if task.running_content:
                # Start to run tasks ...
                self.run_task(task=task)
            else:
                # Keep waiting
                time.sleep(timer.time_interval.check_task)

    def run_task(self, task: Task) -> None:
        """Run the task it directs. It runs the task by meta-data *Task.running_content* and records the running result
        back to meta-data *Task.running_result* and *Task.result_detail*.

        The core implementation of how it works web spider job in protected function *_run_crawling_processing* (and
        *processing_crawling_task*).

        Args:
            task (Task): The task it directs.

        Returns:
            None

        """
        running_content = task.running_content
        current_task: Task = task
        start_task_id = task.in_progressing_id

        assert re.search(r"[0-9]{1,32}", start_task_id) is not None, "The task index must be integer format value."

        for index, content in enumerate(running_content[int(start_task_id) :]):
            content = TaskContentDataUtils.convert_to_running_content(content)

            # Update the ongoing task ID
            original_task = self._get_metadata(path=self._path.task, as_obj=Task)
            if index == 0:
                current_task = Update.task(
                    task=original_task, in_progressing_id=content.task_id, running_status=TaskResult.PROCESSING
                )
            else:
                current_task = Update.task(task=original_task, in_progressing_id=content.task_id)
            self._set_metadata(path=self._path.task, metadata=current_task)

            # Run the task and update the meta data Task
            try:
                # TODO: Consider of here usage about how to implement to let it be more clear and convenience in usage
                #  in client site
                data = self._crawler_process_callback(content)
            except NotImplementedError as e:
                raise e
            except Exception as e:  # pylint: disable=broad-except
                # Update attributes with fail result
                running_result = TaskContentDataUtils.convert_to_running_result(original_task.running_result)
                updated_running_result = RunningResult(
                    success_count=running_result.success_count, fail_count=running_result.fail_count + 1
                )

                result_detail = original_task.result_detail
                # TODO: If it gets fail, how to record the result?
                result_detail.append(
                    ResultDetail(
                        task_id=content.task_id,
                        state=TaskResult.ERROR.value,
                        status_code=500,
                        response=None,
                        error_msg=f"{e}",
                    )
                )
            else:
                # Update attributes with successful result
                running_result = TaskContentDataUtils.convert_to_running_result(original_task.running_result)
                updated_running_result = RunningResult(
                    success_count=running_result.success_count + 1, fail_count=running_result.fail_count
                )

                result_detail = original_task.result_detail
                # TODO: Some information like HTTP status code of response should be get from response object.
                result_detail.append(
                    ResultDetail(
                        task_id=content.task_id,
                        state=TaskResult.DONE.value,
                        status_code=200,
                        response=data,
                        error_msg=None,
                    )
                )

            current_task = Update.task(
                task=original_task,
                in_progressing_id=content.task_id,
                running_result=updated_running_result,
                result_detail=result_detail,
            )
            self._set_metadata(path=self._path.task, metadata=current_task)

        # Finish all tasks, record the running result and reset the content ...
        current_task = Update.task(
            task=current_task, running_content=[], in_progressing_id="-1", running_status=TaskResult.DONE
        )
        self._set_metadata(path=self._path.task, metadata=current_task)


class PrimaryBackupRunnerWorkflow(BaseRoleWorkflow):
    """*The processing of role **BACKUP RUNNER***

    Literally, the crawler role **BACKUP RUNNER** should be the backup of each **RUNNER**. But in this workflow, the
    backup one should be the primary one, which means the index of backup instance's crawler name should be same as the
    value of meta-data *GroupState.standby_id*.

    The primary backup runner would keep monitoring the heartbeat status of each **RUNNER**. It would immediately
    activate itself to be **RUNNER** if it discovers anyone of **RUNNER** is dead, and it would try to hand over the
    dead one's tasks if the dead one doesn't finish its task yet.
    """

    @property
    def role(self) -> CrawlerStateRole:
        return CrawlerStateRole.BACKUP_RUNNER

    def run(self, timer: CrawlerTimer) -> None:
        """Keep checking everyone's heartbeat info, and standby to activate to be a runner by itself if it discovers
        anyone is dead.

        It would record 2 types checking result as dict type value and the data structure like below:
            * timeout records: {<crawler_name>: <timeout times>}
            * not timeout records: {<crawler_name>: <not timeout times>}

        It would record the timeout times into the value, and it would get each types timeout threshold from meta-data
        **Heartbeat**. So it would base on them to run the checking process.

        Prevent from the times be counted by unstable network environment and cause the crawler instance be marked as
        *Dead*, it won't count the times by accumulation, it would reset the timeout value if the record be counted long
        time ago. Currently, it would reset the timeout value if it was counted 10 times ago.

        Args:
            timer (CrawlerTimer): The object which has some timer attributes. In this workflow, it would use 2
                attributes:

                * *CrawlerTimer.time_interval.check_crawler_state* (`float`_)
                    How long does the crawler instance wait a second for next checking heartbeat. The unit is seconds
                    and default value is 0.5.

                * *CrawlerTimer.threshold.reset_timeout* (`int`_)
                    The threshold of how many straight times it doesn't occur, then it would reset the timeout record.

        Returns:
            None

        .. _float: https://docs.python.org/3/library/functions.html#float
        .. _int: https://docs.python.org/3/library/functions.html#int
        """
        timeout_records: Dict[str, int] = {}
        no_timeout_records: Dict[str, int] = {}

        def _one_current_runner_is_dead(runner_name: str) -> Optional[str]:
            current_runner_heartbeat = self._get_metadata(
                path=f"{self._path.generate_parent_node(runner_name)}/{self._path.heartbeat_node_str}",
                as_obj=Heartbeat,
            )

            heart_rhythm_time = current_runner_heartbeat.heart_rhythm_time
            time_format = current_runner_heartbeat.time_format
            update_timeout = current_runner_heartbeat.update_timeout
            heart_rhythm_timeout = current_runner_heartbeat.heart_rhythm_timeout

            diff_datetime = datetime.now() - datetime.strptime(heart_rhythm_time, time_format)
            if diff_datetime.total_seconds() >= parse_timer(update_timeout):
                # It should start to pay attention on it
                timeout_records[runner_name] = timeout_records.get(runner_name, 0) + 1
                no_timeout_records[runner_name] = 0
                if timeout_records[runner_name] >= int(heart_rhythm_timeout):
                    return runner_name
            else:
                no_timeout_records[runner_name] = no_timeout_records.get(runner_name, 0) + 1
                if no_timeout_records[runner_name] >= timer.threshold.reset_timeout:
                    timeout_records[runner_name] = 0
            return None

        while True:
            group_state = self._get_metadata(path=self._path.group_state, as_obj=GroupState)
            chk_current_runners_is_dead = map(_one_current_runner_is_dead, group_state.current_runner)
            dead_current_runner_iter = filter(
                lambda _dead_runner: _dead_runner is not None, list(chk_current_runners_is_dead)
            )
            dead_current_runner = list(dead_current_runner_iter)
            if dead_current_runner:
                # Only handle the first one of dead crawlers (if it has multiple dead crawlers
                runner_name = dead_current_runner[0]
                heartbeat = self._get_metadata(
                    path=f"{self._path.generate_parent_node(runner_name)}/{self._path.heartbeat_node_str}",
                    as_obj=Heartbeat,
                )
                task_of_dead_crawler = self.discover(dead_crawler_name=runner_name, heartbeat=heartbeat)
                self.activate(dead_crawler_name=runner_name)
                self.hand_over_task(task=task_of_dead_crawler)
                break

            time.sleep(timer.time_interval.check_crawler_state)

    def discover(self, dead_crawler_name: str, heartbeat: Heartbeat) -> Task:
        """When backup role crawler instance discover anyone is dead, it would mark the target one as *Dead*
        (*HeartState.Asystole*) and update its meta-data **Heartbeat**. In the same time, it would try to get
        its **Task** and take over it.

        Args:
            dead_crawler_name (str): The crawler name which be under checking.
            heartbeat (Heartbeat): The meta-data **Heartbeat** of crawler be under checking.

        Returns:
            Task: Meta-data **Task** from dead crawler instance.

        """
        node_state_path = f"{self._path.generate_parent_node(dead_crawler_name)}/{self._path.state_node_str}"
        node_state = self._get_metadata(path=node_state_path, as_obj=NodeState)
        node_state.role = CrawlerStateRole.DEAD_RUNNER
        self._set_metadata(path=node_state_path, metadata=node_state)

        task = self._get_metadata(
            path=f"{self._path.generate_parent_node(dead_crawler_name)}/{self._path.task_node_str}",
            as_obj=Task,
        )
        heartbeat.healthy_state = HeartState.ASYSTOLE
        heartbeat.task_state = task.running_status
        self._set_metadata(
            path=f"{self._path.generate_parent_node(dead_crawler_name)}/{self._path.heartbeat_node_str}",
            metadata=heartbeat,
        )

        return task

    def activate(self, dead_crawler_name: str) -> None:
        """After backup role crawler instance marks target as *Dead*, it would start to activate to be running by itself
        and run the runner's job.

        Args:
            dead_crawler_name (str): The crawler name which be under checking.

        Returns:
            None

        """

        def _update_group_state() -> None:
            state = self._get_metadata(path=self._path.group_state, as_obj=GroupState)

            state.total_backup = state.total_backup - 1
            state.current_crawler.remove(dead_crawler_name)
            state.current_runner.remove(dead_crawler_name)
            state.current_runner.append(str(self._crawler_name))
            state.current_backup.remove(str(self._crawler_name))
            state.fail_crawler.append(dead_crawler_name)
            state.fail_runner.append(dead_crawler_name)
            state.standby_id = str(int(state.standby_id) + 1)

            self._set_metadata(path=self._path.group_state, metadata=state)

        node_state = self._get_metadata(path=self._path.node_state, as_obj=NodeState)
        node_state.role = CrawlerStateRole.RUNNER
        self._set_metadata(path=self._path.node_state, metadata=node_state)

        self._lock.strongly_run(function=_update_group_state)

    def hand_over_task(self, task: Task) -> None:
        """Hand over the task of the dead crawler instance. It would get the meta-data **Task** from dead crawler and
        write it to this crawler's meta-data **Task**.

        Args:
            task (Task): The meta-data **Task** of crawler be under checking.

        Returns:
            None

        """
        if task.running_status == TaskResult.PROCESSING.value:
            # Run the tasks from the start index
            self._set_metadata(path=self._path.task, metadata=task)
        elif task.running_status == TaskResult.ERROR.value:
            # Reset some specific attributes
            updated_task = Update.task(
                task,
                in_progressing_id="0",
                running_result=RunningResult(success_count=0, fail_count=0),
                result_detail=[],
            )
            # Reruns all tasks
            self._set_metadata(path=self._path.task, metadata=updated_task)
        else:
            # Ignore and don't do anything if the task state is nothing or done.
            pass


class SecondaryBackupRunnerWorkflow(BaseRoleWorkflow):
    """*The processing of role **BACKUP RUNNER***

    Literally, the crawler role **BACKUP RUNNER** should be the backup of each **RUNNER**. If the backup crawler member
    is not primary, then it is secondary backup runner. The secondary backup crawler only does one thing --- keep
    monitoring and checking whether the *GroupState.standby_id* to be same as the index of current crawler or not. If it
    does, then it would activate itself to be the primary backup runner.
    """

    @property
    def role(self) -> CrawlerStateRole:
        return CrawlerStateRole.BACKUP_RUNNER

    def run(self, timer: CrawlerTimer) -> bool:
        """Keep waiting to be the primary backup crawler instance.

        Args:
            timer (CrawlerTimer): The object which has some timer attributes. In this workflow, it would use only one
                attribute:

                * *CrawlerTimer.time_interval.check_standby_id* (`float`_)
                    How long does the crawler instance wait a second for next checking GroupState.standby_id. The unit
                    is seconds and default value is 2.

        Returns:
            bool: It's True if it directs the standby ID attribute value is equal to its index of name.

        .. _float: https://docs.python.org/3/library/functions.html#float
        """
        while True:
            group_state = self._get_metadata(path=self._path.group_state, as_obj=GroupState)
            if str(self._crawler_name.id) == group_state.standby_id:
                # Start to do wait_and_standby
                return True
            time.sleep(timer.time_interval.check_standby_id)


class HeartbeatUpdatingWorkflow(BaseWorkflow):
    """*The processing of every role which needs to alive with updating heartbeat*

    This is a general workflow, so its super class isn't **BaseRoleWorkflow**, is **BaseWorkflow**. This workflow only
    focus on one thing --- keep updating the current crawler instance's heartbeat.
    """

    _stop_heartbeat_signal: bool = False
    _updating_exception: Exception = None

    @property
    def stop_heartbeat(self) -> bool:
        """:obj:`bool`: Property with getter and setter for stopping updating the current crawler instance's heartbeat
        info.
        """
        return self._stop_heartbeat_signal

    @stop_heartbeat.setter
    def stop_heartbeat(self, signal: bool) -> None:
        self._stop_heartbeat_signal = signal

    def run(self) -> None:
        """The main function of updating **Heartbeat** info.

        .. note::
            It has a flag *_stop_heartbeat_signal*. If it's True, it would stop updating **Heartbeat**.

        Returns:
            None

        """
        while True:
            if not self._stop_heartbeat_signal:
                try:
                    # Get *Task* and *Heartbeat* info
                    task = self._get_metadata(path=self._path.task, as_obj=Task)
                    heartbeat = self._get_metadata(path=self._path.heartbeat, as_obj=Heartbeat)

                    # Update the values
                    heartbeat = Update.heartbeat(
                        heartbeat,
                        heart_rhythm_time=datetime.now(),
                        healthy_state=HeartState.HEALTHY,
                        task_state=task.running_status,
                    )
                    self._set_metadata(path=self._path.heartbeat, metadata=heartbeat)

                    # Sleep ...
                    time.sleep(parse_timer(heartbeat.update_time))
                except Exception as e:  # pylint: disable=broad-except
                    self._updating_exception = e
                    break
            else:
                task = self._get_metadata(path=self._path.task, as_obj=Task)
                heartbeat = self._get_metadata(path=self._path.heartbeat, as_obj=Heartbeat)
                heartbeat = Update.heartbeat(
                    heartbeat,
                    heart_rhythm_time=datetime.now(),
                    healthy_state=HeartState.APPARENT_DEATH,
                    task_state=task.running_status,
                )
                self._set_metadata(path=self._path.heartbeat, metadata=heartbeat)
                break
        if self._updating_exception:
            raise self._updating_exception
