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
    HeartbeatUpdatingWorkflow,
    PrimaryBackupRunnerWorkflow,
    RunnerWorkflow,
    SecondaryBackupRunnerWorkflow,
)
from smoothcrawler_cluster.exceptions import CrawlerIsDeadError
from smoothcrawler_cluster.model import CrawlerRole, GroupState
from smoothcrawler_cluster.model._data import CrawlerName, MetaDataOpt, MetaDataPath


class WorkflowDispatcher:
    """*Dispatcher for each SmoothCrawler-Cluster role to get the role's workflow*

    This object is a dispatcher of dispatching to instantiate different workflow object by different roles. Each role in
    *SmoothCrawler-Cluster* has their own responsibilities and **BaseRunStrategyByCrawlerRole** family integrates all
    the jobs into different objects as different single workflow. **CrawlerRoleDispatcher** would dispatch to generate
    their own workflow object.
    """

    def __init__(
        self,
        name: CrawlerName,
        path: MetaDataPath,
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
        self._crawler_name_data = name
        self._path = path
        self._metadata_opts_callback = metadata_opts_callback
        self._get_metadata = self._metadata_opts_callback.get_callback
        self._lock = lock
        self._crawler_process_callback = crawler_process_callback

    def dispatch(self, role: Union[str, CrawlerRole]) -> Optional[BaseRoleWorkflow]:
        """Dispatch to generate the specific workflow object by the argument *option*.

        Args:
            role (Union[str, CrawlerRole]): The crawler instance's role in *SmoothCrawler-Cluster*.

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
            "name": self._crawler_name_data,
            "path": self._path,
            "metadata_opts_callback": self._metadata_opts_callback,
            "lock": self._lock,
            "crawler_process_callback": self._crawler_process_callback,
        }
        if self._is(role, CrawlerRole.RUNNER):
            return RunnerWorkflow(**role_workflow_args)
        elif self._is(role, CrawlerRole.BACKUP_RUNNER):
            if self._is_primary_backup():
                return PrimaryBackupRunnerWorkflow(**role_workflow_args)
            else:
                return SecondaryBackupRunnerWorkflow(**role_workflow_args)
        else:
            if self._is(role, CrawlerRole.DEAD_RUNNER) or self._is(role, CrawlerRole.DEAD_BACKUP_RUNNER):
                raise CrawlerIsDeadError(crawler_name=str(self._crawler_name_data), group=self._crawler_name_data.group)
            else:
                raise NotImplementedError(f"It doesn't support crawler role {role} in *SmoothCrawler-Cluster*.")

    def heartbeat(self) -> HeartbeatUpdatingWorkflow:
        """Dispatch to a workflow updates heartbeat.

        Returns:
            It would return **HeartbeatUpdatingWorkflow** type instance.

        """
        workflow_args = {
            "name": self._crawler_name_data,
            "path": self._path,
            "metadata_opts_callback": self._metadata_opts_callback,
        }
        return HeartbeatUpdatingWorkflow(**workflow_args)

    @classmethod
    def _is(cls, check: Union[str, CrawlerRole], expected: CrawlerRole) -> bool:
        """Verify whether the input argument *check* is equal to expected value or not.

        Args:
            check (Union[str, CrawlerRole]): The under test value which would be verified.
            expected (CrawlerRole): The expected value we hope the under test value should be.

        Returns:
            It returns *True* if it equals to expected value, nor it returns *False*.

        """
        return (isinstance(check, str) and check == expected.value) or (
            isinstance(check, CrawlerRole) and check is expected
        )

    def _is_primary_backup(self) -> bool:
        """
        Check the current crawler instance is primary backup or secondary backup.

        Returns:
            It's *True* if it is primary backup, nor it is *False*.

        """
        group_state = self._get_metadata(path=self._path.group_state, as_obj=GroupState)
        return self._crawler_name_data.id == group_state.standby_id
