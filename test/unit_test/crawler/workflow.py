from abc import ABCMeta, abstractmethod
from unittest.mock import patch

import pytest

from smoothcrawler_cluster.crawler.workflow import (
    BaseRoleWorkflow,
    PrimaryBackupRunnerWorkflow,
    RunnerWorkflow,
    SecondaryBackupRunnerWorkflow,
)
from smoothcrawler_cluster.model.metadata_enum import CrawlerStateRole


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


class TestPrimaryBackupRunnerWorkflow(BaseRoleWorkflowTestSpec):
    @pytest.fixture(scope="function")
    def workflow(self) -> PrimaryBackupRunnerWorkflow:
        workflow_args = _get_workflow_arguments()
        return PrimaryBackupRunnerWorkflow(**workflow_args)

    @property
    def _expected_role(self) -> CrawlerStateRole:
        return CrawlerStateRole.BACKUP_RUNNER


class TestSecondaryBackupRunnerWorkflow(BaseRoleWorkflowTestSpec):
    @pytest.fixture(scope="function")
    def workflow(self) -> SecondaryBackupRunnerWorkflow:
        workflow_args = _get_workflow_arguments()
        return SecondaryBackupRunnerWorkflow(**workflow_args)

    @property
    def _expected_role(self) -> CrawlerStateRole:
        return CrawlerStateRole.BACKUP_RUNNER
