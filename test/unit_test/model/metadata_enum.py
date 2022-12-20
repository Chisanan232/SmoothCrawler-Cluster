from smoothcrawler_cluster.model.metadata_enum import CrawlerStateRole, TaskResult, HeartState
from enum import Enum
from abc import ABCMeta


class _EnumObjTest(metaclass=ABCMeta):

    @classmethod
    def _run_enum_value_test(cls, under_test_enum: Enum, expected_value: str) -> None:
        assert under_test_enum.value == expected_value, \
            f"The value of enum member '{under_test_enum}' should be '{expected_value}'."


class TestCrawlerStateRole(_EnumObjTest):
    """Test for the enum object key-value mapping."""

    def test_runner_value(self) -> None:
        under_test_enum = CrawlerStateRole.RUNNER
        expected_value = "runner"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_backup_runner_value(self) -> None:
        under_test_enum = CrawlerStateRole.BACKUP_RUNNER
        expected_value = "backup-runner"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_dead_runner_value(self) -> None:
        under_test_enum = CrawlerStateRole.DEAD_RUNNER
        expected_value = "dead-runner"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_dead_backup_runner_value(self) -> None:
        under_test_enum = CrawlerStateRole.DEAD_BACKUP_RUNNER
        expected_value = "dead-backup-runner"
        self._run_enum_value_test(under_test_enum, expected_value)


class TestTaskResult(_EnumObjTest):
    """Test for the enum object key-value mapping."""

    def test_processing_value(self) -> None:
        under_test_enum = TaskResult.PROCESSING
        expected_value = "processing"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_done_value(self) -> None:
        under_test_enum = TaskResult.DONE
        expected_value = "done"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_terminate_value(self) -> None:
        under_test_enum = TaskResult.TERMINATE
        expected_value = "terminate"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_error_value(self) -> None:
        under_test_enum = TaskResult.ERROR
        expected_value = "error"
        self._run_enum_value_test(under_test_enum, expected_value)


class TestHeartState(_EnumObjTest):
    """Test for the enum object key-value mapping."""

    def test_newborn_value(self) -> None:
        under_test_enum = HeartState.NEWBORN
        expected_value = "Newborn"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_healthy_value(self) -> None:
        under_test_enum = HeartState.HEALTHY
        expected_value = "Healthy"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_arrhythmia_value(self) -> None:
        under_test_enum = HeartState.ARRHYTHMIA
        expected_value = "Arrhythmia"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_asystole_value(self) -> None:
        under_test_enum = HeartState.ASYSTOLE
        expected_value = "Asystole"
        self._run_enum_value_test(under_test_enum, expected_value)

    def test_apparent_death_value(self) -> None:
        under_test_enum = HeartState.APPARENT_DEATH
        expected_value = "Apparent Death"
        self._run_enum_value_test(under_test_enum, expected_value)
