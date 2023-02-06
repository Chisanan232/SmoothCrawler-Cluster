"""*Dispatcher for dispatching the workflows*

This module is a dispatcher module, which would help outside caller to get the workflow they need to run in cluster.

In *SmoothCrawler-Cluster*, every role has its own responsibility to do many things. The *workflow* module let each
role's processes to be individual to manage and maintain them, and *dispatcher* module would help caller to get the
process (in generally, it is **BaseRoleWorkflow** type object) from *workflow* module, and they could use the workflow
very easily as following code:

.. code-block:: python

    workflow = WorkflowDispatcher.dispatch(<crawler's current role>)
    workflow.run(timer=<CrawlerTimer object>)

Therefore, it also could control what running strategy it should be in cluster by this *dispatcher* module objects.
"""

from typing import Callable, Optional, Union

from smoothcrawler_cluster.crawler.adapter import DistributedLock
from smoothcrawler_cluster.crawler.workflow import (
    BaseRoleWorkflow,
    BaseWorkflow,
    HeartbeatUpdatingWorkflow,
    PrimaryBackupRunnerWorkflow,
    RunnerWorkflow,
    SecondaryBackupRunnerWorkflow,
)
from smoothcrawler_cluster.exceptions import CrawlerIsDeadError
from smoothcrawler_cluster.model import CrawlerStateRole, GroupState
from smoothcrawler_cluster.model._data import MetaDataPath


class WorkflowDispatcher:
    """*Dispatcher for each SmoothCrawler-Cluster role to get the role's workflow*

    This object is a dispatcher of dispatching to instantiate different workflow object by different roles. Each role in
    *SmoothCrawler-Cluster* has their own responsibilities and **BaseRunStrategyByCrawlerRole** family integrates all
    the jobs into different objects as different single workflow. **CrawlerRoleDispatcher** would dispatch to generate
    their own workflow object.
    """

    def __init__(
        self,
        crawler_name: str,
        group: str,
        index_sep: str,
        path: MetaDataPath,
        get_metadata_callback: Callable,
        set_metadata_callback: Callable,
        opt_metadata_with_lock: DistributedLock,
        crawler_process_callback: Callable,
    ):
        """

        Args:
            crawler_name (str): The current crawler instance's name.
            index_sep (str): The index separation of current crawler instance's name.
            path (Type[MetaDataPath]): The objects which has all meta-data object's path property.
            get_metadata_callback (Callable): The callback function about getting meta-data as object.
            set_metadata_callback (Callable): The callback function about setting meta-data from object.
            opt_metadata_with_lock (DistributedLock): The adapter of distributed lock.
            crawler_process_callback (Callable): The callback function about running the crawler core processes.
        """
        self._crawler_name = crawler_name
        self._group = group
        self._index_sep = index_sep
        self._path = path
        self._get_metadata = get_metadata_callback
        self._set_metadata = set_metadata_callback
        self._opt_metadata_with_lock = opt_metadata_with_lock
        self._crawler_process_callback = crawler_process_callback

    def dispatch(self, role: Union[str, CrawlerStateRole]) -> Optional[BaseRoleWorkflow]:
        """Dispatch to generate the specific workflow object by the argument *option*.

        Args:
            role (Union[str, CrawlerStateRole]): The crawler instance's role in *SmoothCrawler-Cluster*.

        Returns:
            It would return **BaseRoleWorkflow** type object. Below are the mapping table of role with its workflow
            object:

            +-----------------------------+-----------------------------------------+
            |             Role            |             Workflow Object             |
            +-----------------------------+-----------------------------------------+
            |            Runner           |            **RunnerWorkflow**           |
            +-----------------------------+-----------------------------------------+
            |    Primary Backup Runner    |     **PrimaryBackupRunnerWorkflow**     |
            +-----------------------------+-----------------------------------------+
            |   Secondary Backup Runner   |    **SecondaryBackupRunnerWorkflow**    |
            +-----------------------------+-----------------------------------------+

        Raises:
            * NotImplementedError: The role is not **CrawlerStateRole** type.
            * CrawlerIsDeadError: The current crawler instance is dead.

        """
        role_workflow_args = {
            "crawler_name": self._crawler_name,
            "index_sep": self._index_sep,
            "path": self._path,
            "get_metadata": self._get_metadata,
            "set_metadata": self._set_metadata,
            "opt_metadata_with_lock": self._opt_metadata_with_lock,
            "crawler_process_callback": self._crawler_process_callback,
        }
        if self._is(role, CrawlerStateRole.RUNNER):
            return RunnerWorkflow(**role_workflow_args)
        elif self._is(role, CrawlerStateRole.BACKUP_RUNNER):
            if self._is_primary_backup():
                return PrimaryBackupRunnerWorkflow(**role_workflow_args)
            else:
                return SecondaryBackupRunnerWorkflow(**role_workflow_args)
        else:
            if self._is(role, CrawlerStateRole.DEAD_RUNNER) or self._is(role, CrawlerStateRole.DEAD_BACKUP_RUNNER):
                raise CrawlerIsDeadError(crawler_name=self._crawler_name, group=self._group)
            else:
                raise NotImplementedError(f"It doesn't support crawler role {role} in *SmoothCrawler-Cluster*.")

    def heartbeat(self) -> BaseWorkflow:
        """Dispatch to a workflow updates heartbeat.

        Returns:
            It would return **HeartbeatUpdatingWorkflow** type instance.

        """
        workflow_args = {
            "crawler_name": self._crawler_name,
            "index_sep": self._index_sep,
            "path": self._path,
            "get_metadata": self._get_metadata,
            "set_metadata": self._set_metadata,
        }
        return HeartbeatUpdatingWorkflow(**workflow_args)

    @classmethod
    def _is(cls, check: Union[str, CrawlerStateRole], expected: CrawlerStateRole) -> bool:
        """Verify whether the input argument *check* is equal to expected value or not.

        Args:
            check (Union[str, CrawlerStateRole]): The under test value which would be verified.
            expected (CrawlerStateRole): The expected value we hope the under test value should be.

        Returns:
            It returns *True* if it equals to expected value, nor it returns *False*.

        """
        return (isinstance(check, str) and check == expected.value) or (
            isinstance(check, CrawlerStateRole) and check is expected
        )

    def _is_primary_backup(self) -> bool:
        """
        Check the current crawler instance is primary backup or secondary backup.

        Returns:
            It's *True* if it is primary backup, nor it is *False*.

        """
        group_state = self._get_metadata(path=self._path.group_state, as_obj=GroupState)
        return self._crawler_name.split(self._index_sep)[-1] == group_state.standby_id
