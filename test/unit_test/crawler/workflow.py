from abc import ABCMeta, abstractmethod
from unittest.mock import MagicMock, Mock, PropertyMock, patch

import pytest

from smoothcrawler_cluster.crawler.adapter import DistributedLock
from smoothcrawler_cluster.crawler.workflow import (
    BaseRoleWorkflow,
    PrimaryBackupRunnerWorkflow,
    RunnerWorkflow,
    SecondaryBackupRunnerWorkflow,
)
from smoothcrawler_cluster.model import CrawlerStateRole, Task, TaskResult, Update
from smoothcrawler_cluster.model._data import MetaDataPath


def _mock_callable(*args, **kwargs):
    pass


@patch("smoothcrawler_cluster.model._data.MetaDataPath")
@patch("smoothcrawler_cluster.crawler.adapter.DistributedLock")
def _get_workflow_arguments(mock_metadata_path, mock_adapter_lock) -> dict:
    workflow_args = {
        "crawler_name": "test_name",
        "index_sep": "test_index_sep",
        "path": mock_metadata_path,
        "get_metadata": _mock_callable,
        "set_metadata": _mock_callable,
        "opt_metadata_with_lock": mock_adapter_lock,
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
    def _expected_role(self) -> CrawlerStateRole:
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
    def _expected_role(self) -> CrawlerStateRole:
        return CrawlerStateRole.RUNNER

    def test_run_task(self):
        # TODO: Test the exception parts
        pass


class TestPrimaryBackupRunnerWorkflow(BaseRoleWorkflowTestSpec):
    @pytest.fixture(scope="function")
    def workflow(self) -> PrimaryBackupRunnerWorkflow:
        workflow_args = _get_workflow_arguments()
        return PrimaryBackupRunnerWorkflow(**workflow_args)

    @property
    def _expected_role(self) -> CrawlerStateRole:
        return CrawlerStateRole.BACKUP_RUNNER

    def test_hand_over_task_with_processing(self):
        # Mock function what it needs
        mock_task = Mock(Task())
        mock_task.running_status = TaskResult.PROCESSING.value

        mock_metadata_path = MagicMock(MetaDataPath(name="test_name", group="test_group"))
        prop_task = PropertyMock(return_value="test_task_path")
        type(mock_metadata_path).task = prop_task

        mock_distributed_lock = Mock(DistributedLock(lock=Mock()))

        workflow_args = {
            "crawler_name": "test_name",
            "index_sep": "test_index_sep",
            "path": mock_metadata_path,
            "get_metadata": _mock_callable,
            "set_metadata": _mock_callable,
            "opt_metadata_with_lock": mock_distributed_lock,
            "crawler_process_callback": _mock_callable,
        }
        workflow = PrimaryBackupRunnerWorkflow(**workflow_args)
        workflow._set_metadata = MagicMock(return_value=None)

        # Run the target function to test
        workflow.hand_over_task(mock_task)

        # Verify the target function running result
        prop_task.assert_called_once()
        workflow._set_metadata.assert_called_once_with(path=type(mock_metadata_path).task, metadata=mock_task)


class TestSecondaryBackupRunnerWorkflow(BaseRoleWorkflowTestSpec):
    @pytest.fixture(scope="function")
    def workflow(self) -> SecondaryBackupRunnerWorkflow:
        workflow_args = _get_workflow_arguments()
        return SecondaryBackupRunnerWorkflow(**workflow_args)

    @property
    def _expected_role(self) -> CrawlerStateRole:
        return CrawlerStateRole.BACKUP_RUNNER
