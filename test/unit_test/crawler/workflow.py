import re
from abc import ABCMeta, abstractmethod
from typing import TypeVar
from unittest.mock import MagicMock, Mock, PropertyMock, call, patch

import pytest

from smoothcrawler_cluster.crawler.adapter import DistributedLock
from smoothcrawler_cluster.crawler.workflow import (
    BaseRoleWorkflow,
    PrimaryBackupRunnerWorkflow,
    RunnerWorkflow,
    SecondaryBackupRunnerWorkflow,
)
from smoothcrawler_cluster.model import (
    CrawlerRole,
    RunningResult,
    Task,
    TaskState,
    Update,
)
from smoothcrawler_cluster.model._data import CrawlerName, MetaDataOpt, MetaDataPath

from ..._values import (
    _One_Running_Content_As_Object,
    _Task_In_Progressing_Id_Value,
    _Task_Running_Content_Value,
    _Task_Running_Result,
)

BaseRoleWorkflowType = TypeVar("BaseRoleWorkflowType", bound=BaseRoleWorkflow)


def _mock_callable(*args, **kwargs):
    pass


@patch("smoothcrawler_cluster.model._data.CrawlerName")
@patch("smoothcrawler_cluster.model._data.MetaDataOpt")
@patch("smoothcrawler_cluster.model._data.MetaDataPath")
@patch("smoothcrawler_cluster.crawler.adapter.DistributedLock")
def _get_workflow_arguments(mock_name, mock_metadata_opts, mock_metadata_path, mock_adapter_lock) -> dict:
    workflow_args = {
        "name": mock_name,
        "path": mock_metadata_path,
        "metadata_opts_callback": mock_metadata_opts,
        "lock": mock_adapter_lock,
        "crawler_process_callback": _mock_callable,
    }
    return workflow_args


class BaseRoleWorkflowTestSpec(metaclass=ABCMeta):
    @pytest.fixture(scope="function")
    @abstractmethod
    def workflow(self) -> BaseRoleWorkflow:
        pass

    @property
    @abstractmethod
    def _expected_role(self) -> CrawlerRole:
        pass

    def test_role(self, workflow: BaseRoleWorkflow):
        assert (
            workflow.role is self._expected_role
        ), f"It should be crawler role {self._expected_role.value}, but it is {workflow.role}."


class TestRunnerWorkflow(BaseRoleWorkflowTestSpec):
    @pytest.fixture(scope="function")
    def workflow(self) -> RunnerWorkflow:
        workflow_args = _get_workflow_arguments()
        return RunnerWorkflow(**workflow_args)

    @property
    def _expected_role(self) -> CrawlerRole:
        return CrawlerRole.RUNNER

    def test_run_task_not_implemented_error(self):
        # Mock function what it needs
        def _raise_error_process(*args, **kwargs) -> None:
            raise NotImplementedError("NotImplementedError by PyTest")

        mock_task = Mock(Task())
        mock_task.running_status = TaskState.PROCESSING.value
        mock_task.running_content = _Task_Running_Content_Value
        mock_task.in_progressing_id = int(_Task_In_Progressing_Id_Value)
        mock_task.running_result = _Task_Running_Result

        mock_crawler_name = Mock(CrawlerName())

        mock_metadata_path = MagicMock(MetaDataPath(name="test_name_1", group="test_group"))
        prop_task = PropertyMock(return_value="test_task_path")
        type(mock_metadata_path).task = prop_task

        mock_metadata_opts = Mock(MetaDataOpt())

        mock_distributed_lock = Mock(DistributedLock(lock=Mock()))

        workflow_args = {
            "name": mock_crawler_name,
            "path": mock_metadata_path,
            "metadata_opts_callback": mock_metadata_opts,
            "lock": mock_distributed_lock,
            "crawler_process_callback": _raise_error_process,
        }
        workflow = RunnerWorkflow(**workflow_args)
        workflow._get_metadata = MagicMock(return_value=mock_task)
        workflow._set_metadata = MagicMock(return_value=None)

        # Run the target function to test
        try:
            with patch.object(re, "search", return_value="1") as mock_re_search:
                with patch.object(Update, "task", return_value=mock_task) as mock_update_task:
                    workflow.run_task(mock_task)
        except NotImplementedError as e:
            assert (
                str(e) == "NotImplementedError by PyTest"
            ), "It should raise an NotImplementedError and content is 'NotImplementedError by PyTest'."
        else:
            assert False, "It should raise an NotImplementedError."

        # Verify the target function running result
        mock_re_search.assert_called_once_with(
            r"[0-9]{1,32}", mock_task.running_content[mock_task.in_progressing_id]["task_id"]
        )
        mock_update_task.assert_called_once_with(
            task=mock_task,
            in_progressing_id=_One_Running_Content_As_Object.task_id,
            running_status=TaskState.PROCESSING,
        )
        prop_task.assert_has_calls([call for _ in range(2)])
        workflow._get_metadata.assert_called_once_with(path=type(mock_metadata_path).task, as_obj=Task)
        workflow._set_metadata.assert_called_once_with(path=type(mock_metadata_path).task, metadata=mock_task)


class TestPrimaryBackupRunnerWorkflow(BaseRoleWorkflowTestSpec):
    @pytest.fixture(scope="function")
    def workflow(self) -> PrimaryBackupRunnerWorkflow:
        workflow_args = _get_workflow_arguments()
        return PrimaryBackupRunnerWorkflow(**workflow_args)

    @property
    def _expected_role(self) -> CrawlerRole:
        return CrawlerRole.BACKUP_RUNNER

    def test_hand_over_task_with_processing(self):
        # Mock function what it needs
        mock_task = self._mock_task(TaskState.PROCESSING)
        mock_metadata_path, prop_task = self._mock_prop_metadata_task_path()
        mock_distributed_lock = self._mock_distributed_lock()

        # Run the target function to test
        workflow = self._init_workflow_and_mock_set_metadata_func(mock_metadata_path, mock_distributed_lock)
        workflow.hand_over_task(mock_task)

        # Verify the target function running result
        prop_task.assert_called_once()
        workflow._set_metadata.assert_called_once_with(path=type(mock_metadata_path).task, metadata=mock_task)

    def test_hand_over_task_with_error(self):
        # Mock function what it needs
        mock_task = self._mock_task(TaskState.ERROR)
        mock_metadata_path, prop_task = self._mock_prop_metadata_task_path()
        mock_distributed_lock = self._mock_distributed_lock()

        workflow = self._init_workflow_and_mock_set_metadata_func(mock_metadata_path, mock_distributed_lock)

        with patch.object(Update, "task", return_value=mock_task) as mock_update_task:
            # Run the target function to test
            workflow.hand_over_task(mock_task)

            # Verify the target function running result
            mock_update_task.assert_called_once_with(
                mock_task,
                in_progressing_id="0",
                running_result=RunningResult(success_count=0, fail_count=0),
                result_detail=[],
            )
            prop_task.assert_called_once()
            workflow._set_metadata.assert_called_once_with(path=type(mock_metadata_path).task, metadata=mock_task)

    @pytest.mark.parametrize("task_state", [TaskState.NOTHING, TaskState.TERMINATE, TaskState.DONE])
    def test_hand_over_task_with_other_status(self, task_state: TaskState):
        # Mock function what it needs
        mock_task = self._mock_task(task_state)
        mock_metadata_path, prop_task = self._mock_prop_metadata_task_path()
        mock_distributed_lock = self._mock_distributed_lock()

        # Run the target function to test
        workflow = self._init_workflow_and_mock_set_metadata_func(mock_metadata_path, mock_distributed_lock)
        workflow.hand_over_task(mock_task)

        # Verify the target function running result
        prop_task.assert_not_called()
        workflow._set_metadata.assert_not_called()

    def _mock_task(self, task_state: TaskState) -> Mock:
        mock_task = Mock(Task())
        mock_task.running_status = task_state.value
        return mock_task

    def _mock_prop_metadata_task_path(self) -> (MagicMock, PropertyMock):
        mock_metadata_path = MagicMock(MetaDataPath(name="test_name", group="test_group"))
        prop_task = PropertyMock(return_value="test_task_path")
        type(mock_metadata_path).task = prop_task
        return mock_metadata_path, prop_task

    def _mock_distributed_lock(self) -> Mock:
        return Mock(DistributedLock(lock=Mock()))

    def _init_workflow_and_mock_set_metadata_func(
        self, metadata_path: Mock, adapter_lock: Mock
    ) -> PrimaryBackupRunnerWorkflow:
        mock_crawler_name = Mock(CrawlerName())
        mock_metadata_opts = Mock(MetaDataOpt())

        workflow_args = {
            "name": mock_crawler_name,
            "path": metadata_path,
            "metadata_opts_callback": mock_metadata_opts,
            "lock": adapter_lock,
            "crawler_process_callback": _mock_callable,
        }
        role_workflow = PrimaryBackupRunnerWorkflow(**workflow_args)
        role_workflow._set_metadata = MagicMock(return_value=None)
        return role_workflow


class TestSecondaryBackupRunnerWorkflow(BaseRoleWorkflowTestSpec):
    @pytest.fixture(scope="function")
    def workflow(self) -> SecondaryBackupRunnerWorkflow:
        workflow_args = _get_workflow_arguments()
        return SecondaryBackupRunnerWorkflow(**workflow_args)

    @property
    def _expected_role(self) -> CrawlerRole:
        return CrawlerRole.BACKUP_RUNNER
