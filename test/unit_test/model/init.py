from smoothcrawler_cluster.model import (
    Empty, Initial, Update,
    GroupState,
    CrawlerStateRole, TaskResult, HeartState,
    RunningResult
)
from datetime import datetime

from ..._assertion import (
    WorkingTime,
    ObjectIsNoneOrNotAssertion, MetaDataValueAssertion, ListSizeAssertion
)
from ..._values import (
    _Crawler_Name_Value, _Crawler_Group_Name_Value, _Total_Crawler_Value, _Runner_Crawler_Value, _Backup_Crawler_Value,
    _Task_Running_Content_Value, _Task_Result_Detail_Value
)


class TestEmpty:

    def test_group_state(self):
        # Operate target method for testing
        _state = Empty.group_state()

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_crawler", expected_value=0)
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_runner", expected_value=0)
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_backup", expected_value=0)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="standby_id", expected_value="0")

        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_crawler", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_backup", expected_value=0)

        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_crawler", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="fail_backup", expected_value=0)

    def test_node_state(self):
        # Operate target method for testing
        _state = Empty.node_state()

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="group", expected_value="")
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="role", expected_value=CrawlerStateRole.Initial.value)

    def test_task(self):
        # Operate target method for testing
        _task = Empty.task()

        # Verify values
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_content", expected_value=[])
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="cookie", expected_value={})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="authorization", expected_value={})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="in_progressing_id", expected_value="-1")
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_result", expected_value={'success_count': 0, 'fail_count': 0})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_status", expected_value=TaskResult.Nothing.value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="result_detail", expected_value=[])

    def test_heartbeat(self):
        # Operate target method for testing
        _heartbeat = Empty.heartbeat()

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _heartbeat, is_none=False)


class TestInitial:

    def test_group_state(self):
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

    def test_node_state(self):
        # Operate target method for testing
        _state = Initial.node_state(group="test-group")

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="group", expected_value="test-group")
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="role", expected_value=CrawlerStateRole.Initial.value)

    def test_task(self):
        # Operate target method for testing
        _task = Initial.task()

        # Verify values
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_content", expected_value=[])
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="cookie", expected_value={})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="authorization", expected_value={})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="in_progressing_id", expected_value="-1")
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_result", expected_value={'success_count': 0, 'fail_count': 0})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_status", expected_value=TaskResult.Nothing.value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="result_detail", expected_value=[])

    def test_heartbeat(self):
        # Operate target method for testing
        _heartbeat = Initial.heartbeat()

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _heartbeat, is_none=False)


class TestUpdate:

    def test_group_state(self):
        _test_crawler_name = ["test_crawler_0"]
        _test_standby_id = "1"

        def _chk_list_len_and_ele(state: GroupState, metadata_attr: str) -> None:
            ListSizeAssertion(WorkingTime.AtInitial, state, metadata=metadata_attr, expected_value=1)
            assert getattr(state, metadata_attr) == _test_crawler_name, \
                f"In initialing process, meta data *GroupState.{metadata_attr}* should save value '{_Crawler_Name_Value}'."

        # Operate target method for testing
        _init_state = Initial.group_state(crawler_name=_Crawler_Name_Value, total_crawler=_Total_Crawler_Value, total_runner=_Runner_Crawler_Value, total_backup=_Backup_Crawler_Value)
        _state = Update.group_state(
            _init_state,
            total_crawler=_Total_Crawler_Value + 1,
            total_runner=_Runner_Crawler_Value + 1,
            total_backup=_Backup_Crawler_Value + 1,
            standby_id=_test_standby_id,
            append_current_crawler=_test_crawler_name,
            append_current_runner=_test_crawler_name,
            append_current_backup=_test_crawler_name,
            append_fail_crawler=_test_crawler_name,
            append_fail_runner=_test_crawler_name,
            append_fail_backup=_test_crawler_name
        )

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_crawler", expected_value=_Total_Crawler_Value + 1)
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_runner", expected_value=_Runner_Crawler_Value + 1)
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="total_backup", expected_value=_Backup_Crawler_Value + 1)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="standby_id", expected_value=_test_standby_id)

        ListSizeAssertion(WorkingTime.AtInitial, _state, metadata="current_crawler", expected_value=2)
        _chk_list_len_and_ele(_state, metadata_attr="current_runner")
        _chk_list_len_and_ele(_state, metadata_attr="current_backup")

        _chk_list_len_and_ele(_state, metadata_attr="fail_crawler")
        _chk_list_len_and_ele(_state, metadata_attr="fail_runner")
        _chk_list_len_and_ele(_state, metadata_attr="fail_backup")

    def test_node_state(self):
        # Operate target method for testing
        _init_state = Initial.node_state()
        _state = Update.node_state(_init_state, group=_Crawler_Group_Name_Value, role=CrawlerStateRole.Runner)

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="group", expected_value=_Crawler_Group_Name_Value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _state, metadata="role", expected_value=CrawlerStateRole.Runner.value)

    def test_task(self):
        _test_cookie = {"test_cookie": "test_cookie"}
        _test_auth = {"test_auth": "test_auth"}

        # Operate target method for testing
        _init_task = Initial.task()
        _task = Update.task(
            _init_task,
            running_content=_Task_Running_Content_Value,
            cookie=_test_cookie,
            authorization=_test_auth,
            in_progressing_id="1",
            running_result=RunningResult(success_count=1, fail_count=0),
            running_status=TaskResult.Processing,
            result_detail=_Task_Result_Detail_Value
        )

        # Verify values
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_content", expected_value=_Task_Running_Content_Value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="cookie", expected_value=_test_cookie)
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="authorization", expected_value=_test_auth)
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="in_progressing_id", expected_value="1")
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_result", expected_value={'success_count': 1, 'fail_count': 0})
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="running_status", expected_value=TaskResult.Processing.value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _task, metadata="result_detail", expected_value=_Task_Result_Detail_Value)

    def test_heartbeat(self):
        _test_datetime_now = datetime.now()
        _test_time_format = "%Y/%m/%d %H:%M:%S"
        _test_update_time = "5s"
        _test_update_timeout = "10s"
        _test_heart_rhythm_timeout = "2"

        # Operate target method for testing
        _init_heartbeat = Initial.heartbeat()
        _heartbeat = Update.heartbeat(
            _init_heartbeat,
            heart_rhythm_time=_test_datetime_now,
            time_format=_test_time_format,
            update_time=_test_update_time,
            update_timeout=_test_update_timeout,
            heart_rhythm_timeout=_test_heart_rhythm_timeout,
            healthy_state=HeartState.Healthy,
            task_state=TaskResult.Processing
        )

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AtInitial, _heartbeat, is_none=False)

        MetaDataValueAssertion(WorkingTime.AtInitial, _heartbeat, metadata="heart_rhythm_time", expected_value=_test_datetime_now.strftime(_test_time_format))
        MetaDataValueAssertion(WorkingTime.AtInitial, _heartbeat, metadata="time_format", expected_value=_test_time_format)
        MetaDataValueAssertion(WorkingTime.AtInitial, _heartbeat, metadata="update_time", expected_value=_test_update_time)
        MetaDataValueAssertion(WorkingTime.AtInitial, _heartbeat, metadata="update_timeout", expected_value=_test_update_timeout)
        MetaDataValueAssertion(WorkingTime.AtInitial, _heartbeat, metadata="heart_rhythm_timeout", expected_value=_test_heart_rhythm_timeout)
        MetaDataValueAssertion(WorkingTime.AtInitial, _heartbeat, metadata="healthy_state", expected_value=HeartState.Healthy.value)
        MetaDataValueAssertion(WorkingTime.AtInitial, _heartbeat, metadata="task_state", expected_value=TaskResult.Processing.value)

