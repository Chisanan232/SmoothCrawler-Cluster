from smoothcrawler_cluster.model import CrawlerStateRole, TaskResult
from enum import Enum
from abc import ABCMeta


class _EnumObjTest(metaclass=ABCMeta):

    @classmethod
    def _run_enum_value_test(cls, under_test_enum: Enum, expected_value: str) -> None:
        assert under_test_enum.value == expected_value, f"The value of enum member '{under_test_enum}' should be '{expected_value}'."


class TestCrawlerStateRole(_EnumObjTest):
    """Test for the enum object key-value mapping."""

    def test_runner_value(self) -> None:
        _under_test_enum = CrawlerStateRole.Runner
        _expected_value = "runner"
        self._run_enum_value_test(_under_test_enum, _expected_value)

    def test_backup_runner_value(self) -> None:
        _under_test_enum = CrawlerStateRole.Backup_Runner
        _expected_value = "backup-runner"
        self._run_enum_value_test(_under_test_enum, _expected_value)

    def test_dead_runner_value(self) -> None:
        _under_test_enum = CrawlerStateRole.Dead_Runner
        _expected_value = "dead-runner"
        self._run_enum_value_test(_under_test_enum, _expected_value)

    def test_dead_backup_runner_value(self) -> None:
        _under_test_enum = CrawlerStateRole.Dead_Backup_Runner
        _expected_value = "dead-backup-runner"
        self._run_enum_value_test(_under_test_enum, _expected_value)


class TestTaskResult(_EnumObjTest):
    """Test for the enum object key-value mapping."""

    def test_processing_value(self) -> None:
        _under_test_enum = TaskResult.Processing
        _expected_value = "processing"
        self._run_enum_value_test(_under_test_enum, _expected_value)

    def test_done_value(self) -> None:
        _under_test_enum = TaskResult.Done
        _expected_value = "done"
        self._run_enum_value_test(_under_test_enum, _expected_value)

    def test_terminate_value(self) -> None:
        _under_test_enum = TaskResult.Terminate
        _expected_value = "terminate"
        self._run_enum_value_test(_under_test_enum, _expected_value)

    def test_error_value(self) -> None:
        _under_test_enum = TaskResult.Error
        _expected_value = "error"
        self._run_enum_value_test(_under_test_enum, _expected_value)