"""*Register the metadata to crawler cluster*

This module's responsibility is registering all needed meta-data objects to crawler cluster.

It's possible that have other strategies to run registration through different way, i.e., checking the identity index
and judging whether it is **RUNNER** or not every time when it has anything info updated of **GroupState**. So for the
management and extension, let registration processes to be a single one module in *SmoothCrawler-Cluster*.

*New in version 0.2.0.*
"""

import time

from .crawler.adapter import DistributedLock
from .model import GroupState, Initial, Update
from .model._data import CrawlerName, MetaDataOpt, MetaDataPath


class Register:
    """*General registration*

    This registration strategy just register all needed meta-data objects directly to crawler cluster.
    """

    _initial_standby_id: str = "0"

    def __init__(
        self,
        name: CrawlerName,
        path: MetaDataPath,
        metadata_opts_callback: MetaDataOpt,
        lock: DistributedLock,
    ):
        """

        Args:
            name (CrawlerName): The data object **CrawlerName** which provides some attribute like crawler instance's
                name or ID, etc.
            path (Type[MetaDataPath]): The objects which has all meta-data object's path property.
            metadata_opts_callback (MetaDataOpt): The data object *MetaDataOpt* which provides multiple callback
                functions about getting and setting meta-data.
            lock (DistributedLock): The adapter of distributed lock.
        """
        self._crawler_name_data = name
        self._path = path
        self._exist_metadata = metadata_opts_callback.exist_callback
        self._get_metadata = metadata_opts_callback.get_callback
        self._set_metadata = metadata_opts_callback.set_callback
        self._lock = lock
        # TODO (election): Add one more initial argument about election object.

    def metadata(
        self,
        runner: int,
        backup: int,
        ensure: bool = False,
        ensure_timeout: int = 3,
        ensure_wait: float = 0.5,
        update_time: float = None,
        update_timeout: float = None,
        heart_rhythm_timeout: int = None,
        time_format: str = None,
    ) -> None:
        """

        Args:
            runner (int): The amount of crawler role **RUNNER**.
            backup (int): The amount of crawler role **BACKUP RUNNER**.
            ensure (bool): If it's True, it would guarantee the value of register meta-data processing is satisfied of
                size of *GroupState.current_crawler* is equal to the total of runner and backup, and this crawler name
                must be in it.
            ensure_timeout (int): The times of timeout to guarantee the register meta-data processing finish. Default
                value is 3.
            ensure_wait (float): How long to wait between every checking. Default value is 0.5 (unit is second).
            update_time (float): The time frequency to update heartbeat info, i.g., if value is '2', it would update
                heartbeat info every 2 seconds. The unit is seconds.
            update_timeout (float): The timeout value of updating, i.g., if value is '3', it is time out if it doesn't
                to update heartbeat info exceeds 3 seconds. The unit is seconds.
            heart_rhythm_timeout (int): The threshold of timeout times to judge it is dead, i.g., if value is '3' and
                the updating timeout exceeds 3 times, it would be marked as 'Dead_<Role>' (like 'Dead_Runner' or
                'Dead_Backup').
            time_format (str): The time format. This format rule is same as 'datetime'.

        Returns:
            None.

        """
        self.group_state(
            runner=runner,
            backup=backup,
            ensure=ensure,
            ensure_wait=ensure_wait,
            ensure_timeout=ensure_timeout,
        )
        self.node_state()
        self.task()
        self.heartbeat(
            update_time=update_time,
            update_timeout=update_timeout,
            heart_rhythm_timeout=heart_rhythm_timeout,
            time_format=time_format,
        )

    def group_state(
        self,
        runner: int,
        backup: int,
        ensure: bool = False,
        ensure_timeout: int = 3,
        ensure_wait: float = 0.5,
    ) -> None:
        """Register meta-data *GroupState* to crawler cluster.

        Args:
            runner (int): The number of crawler to run task. This value is equal to attribute *GroupState.total_runner*.
            backup (int): The number of crawler to check all crawler runner is alive or not and standby to activate by
                itself to be another runner if anyone is dead. This value is equal to attribute GroupState.total_backup.
            ensure (bool): If it's True, it would guarantee the value of register meta-data processing is satisfied of
                size of *GroupState.current_crawler* is equal to the total of runner and backup, and this crawler name
                must be in it.
            ensure_timeout (int): The times of timeout to guarantee the register meta-data processing finish. Default
                value is 3.
            ensure_wait (float): How long to wait between every checking. Default value is 0.5 (unit is second).

        Returns:
            None.

        """

        def update_group_state() -> bool:
            """This function be run in distributed lock. About Zookeeper distributed lock behavior, it would initial
            the target node first which be specified to be run in lock. So the node which would be used under lock MUST
            exist. So it remove the initialization process in pacakge version 0.2.0.

            Returns:
                A boolean type value. Returns *True* if it works finely without any issue, nor it would return *False*.

            """
            # TODO (election): Think about adding election process in this running.
            state = self._get_metadata(path=self._path.group_state, as_obj=GroupState)
            if not state.current_crawler or str(self._crawler_name_data) not in state.current_crawler:
                state = Update.group_state(
                    state,
                    total_crawler=runner + backup,
                    total_runner=runner,
                    total_backup=backup,
                    append_current_crawler=[str(self._crawler_name_data)],
                    standby_id=self._initial_standby_id,
                )
                self._set_metadata(path=self._path.group_state, metadata=state)

            if not ensure:
                return True

            state = self._get_metadata(path=self._path.group_state, as_obj=GroupState)
            assert state, "The meta data *State* should NOT be None."
            if (
                len(set(state.current_crawler)) == runner + backup
                and str(self._crawler_name_data) in state.current_crawler
            ):
                return True
            return False

        for _ in range(ensure_timeout):
            result_is_ok = self._lock.strongly_run(function=update_group_state)
            if result_is_ok:
                break
            if ensure_wait:
                time.sleep(ensure_wait)
        else:
            raise TimeoutError("It gets timeout of registering meta data *GroupState* to Zookeeper cluster.")

    def node_state(self) -> None:
        """Register meta-data *NodeState* to crawler cluster.

        Returns:
            None

        """
        state = Initial.node_state(group=self._crawler_name_data.group)
        create_node = not self._exist_metadata(path=self._path.node_state)
        self._set_metadata(path=self._path.node_state, metadata=state, create_node=create_node)

    def task(self) -> None:
        """Register meta-data *Task* to crawler cluster.

        Returns:
            None

        """
        task = Initial.task()
        create_node = not self._exist_metadata(path=self._path.task)
        self._set_metadata(path=self._path.task, metadata=task, create_node=create_node)

    def heartbeat(
        self,
        update_time: float = None,
        update_timeout: float = None,
        heart_rhythm_timeout: int = None,
        time_format: str = None,
    ) -> None:
        """Register meta-data *Heartbeat* to crawler cluster.

        Args:
            update_time (float): The time frequency to update heartbeat info, i.g., if value is '2', it would update
                heartbeat info every 2 seconds. The unit is seconds.
            update_timeout (float): The timeout value of updating, i.g., if value is '3', it is time out if it doesn't
                to update heartbeat info exceeds 3 seconds. The unit is seconds.
            heart_rhythm_timeout (int): The threshold of timeout times to judge it is dead, i.g., if value is '3' and
                the updating timeout exceeds 3 times, it would be marked as 'Dead_<Role>' (like 'Dead_Runner' or
                'Dead_Backup').
            time_format (str): The time format. This format rule is same as 'datetime'.

        Returns:
            None

        """
        update_time = f"{update_time}s" if update_time else None
        update_timeout = f"{update_timeout}s" if update_timeout else None
        heart_rhythm_timeout = f"{heart_rhythm_timeout}" if heart_rhythm_timeout else None
        heartbeat = Initial.heartbeat(
            update_time=update_time,
            update_timeout=update_timeout,
            heart_rhythm_timeout=heart_rhythm_timeout,
            time_format=time_format,
        )
        create_node = not self._exist_metadata(path=self._path.heartbeat)
        self._set_metadata(path=self._path.heartbeat, metadata=heartbeat, create_node=create_node)
