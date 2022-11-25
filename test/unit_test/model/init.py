from smoothcrawler_cluster.model import Initial, CrawlerStateRole, TaskResult

from ..._assertion import (
    WorkingTime,
    ObjectIsNoneOrNotAssertion, MetaDataValueAssertion, ListSizeAssertion
)
from ..._values import _Crawler_Name_Value, _Total_Crawler_Value, _Runner_Crawler_Value, _Backup_Crawler_Value


class TestInitial:

    def test__initial_state(self):
        # Operate target method for testing
        _state = Initial.group_state(crawler_name=_Crawler_Name_Value, total_crawler=_Total_Crawler_Value, total_runner=_Runner_Crawler_Value, total_backup=_Backup_Crawler_Value)

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_crawler", expected_value=_Runner_Crawler_Value + _Backup_Crawler_Value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_runner", expected_value=_Runner_Crawler_Value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_backup", expected_value=_Backup_Crawler_Value)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="standby_id", expected_value="0")

        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_crawler", expected_value=1)
        assert _state.current_crawler[0] == _Crawler_Name_Value, \
            f"In initialing process, meta data *state.current_crawler* should save value '{_Crawler_Name_Value}'."
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_backup", expected_value=0)

        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_crawler", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_backup", expected_value=0)

    def test__initial_node_state(self):
        # Operate target method for testing
        _state = Initial.node_state(group="test-group")

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="group", expected_value="test-group")
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="role", expected_value=CrawlerStateRole.Initial.value)

    def test__initial_task(self):
        # Operate target method for testing
        _task = Initial.task()

        # Verify values
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_content", expected_value=[])
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="cookie", expected_value={})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="authorization", expected_value={})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="in_progressing_id", expected_value="-1")
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_result", expected_value={})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_status", expected_value=TaskResult.Nothing.value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="result_detail", expected_value=[])

    def test__initial_heartbeat(self):
        # Operate target method for testing
        _heartbeat = Initial.heartbeat()

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _heartbeat, is_none=False)
