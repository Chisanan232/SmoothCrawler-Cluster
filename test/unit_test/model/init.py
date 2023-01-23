from datetime import datetime
from smoothcrawler_cluster.model import (
    Empty, Initial, Update,
    GroupState,
    CrawlerStateRole, TaskResult, HeartState,
    RunningResult
)

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
        state = Empty.group_state()

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state, metadata="total_crawler", expected_value=0)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state, metadata="total_runner", expected_value=0)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state, metadata="total_backup", expected_value=0)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state, metadata="standby_id", expected_value="0")

        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="current_crawler", expected_value=0)
        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="current_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="current_backup", expected_value=0)

        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="fail_crawler", expected_value=0)
        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="fail_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="fail_backup", expected_value=0)

    def test_node_state(self):
        # Operate target method for testing
        state = Empty.node_state()

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state, metadata="group", expected_value="")
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="role", expected_value=CrawlerStateRole.INITIAL.value)

    def test_task(self):
        # Operate target method for testing
        task = Empty.task()

        # Verify values
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="running_content", expected_value=[])
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="cookie", expected_value={})
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="authorization", expected_value={})
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="in_progressing_id", expected_value="-1")
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task,
                               metadata="running_result", expected_value={"success_count": 0, "fail_count": 0})
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task,
                               metadata="running_status", expected_value=TaskResult.NOTHING.value)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="result_detail", expected_value=[])

    def test_heartbeat(self):
        # Operate target method for testing
        heartbeat = Empty.heartbeat()

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, heartbeat, is_none=False)


class TestInitial:

    def test_group_state(self):
        # Operate target method for testing
        state = Initial.group_state(crawler_name=_Crawler_Name_Value,
                                    total_crawler=_Total_Crawler_Value,
                                    total_runner=_Runner_Crawler_Value,
                                    total_backup=_Backup_Crawler_Value)

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="total_crawler", expected_value=_Runner_Crawler_Value + _Backup_Crawler_Value)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="total_runner", expected_value=_Runner_Crawler_Value)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="total_backup", expected_value=_Backup_Crawler_Value)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state, metadata="standby_id", expected_value="0")

        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="current_crawler", expected_value=1)
        assert state.current_crawler[0] == _Crawler_Name_Value, \
            f"In initialing process, meta data *state.current_crawler* should save value '{_Crawler_Name_Value}'."
        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="current_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="current_backup", expected_value=0)

        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="fail_crawler", expected_value=0)
        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="fail_runner", expected_value=0)
        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="fail_backup", expected_value=0)

    def test_node_state(self):
        # Operate target method for testing
        state = Initial.node_state(group="test-group")

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state, metadata="group", expected_value="test-group")
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="role", expected_value=CrawlerStateRole.INITIAL.value)

    def test_task(self):
        # Operate target method for testing
        task = Initial.task()

        # Verify values
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="running_content", expected_value=[])
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="cookie", expected_value={})
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="authorization", expected_value={})
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="in_progressing_id", expected_value="-1")
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task,
                               metadata="running_result", expected_value={"success_count": 0, "fail_count": 0})
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task,
                               metadata="running_status", expected_value=TaskResult.NOTHING.value)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="result_detail", expected_value=[])

    def test_heartbeat(self):
        # Operate target method for testing
        heartbeat = Initial.heartbeat()

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, heartbeat, is_none=False)


class TestUpdate:

    def test_group_state(self):
        test_crawler_name = ["test_crawler_0"]
        test_standby_id = "1"

        def _chk_list_len_and_ele(s: GroupState, metadata_attr: str) -> None:
            ListSizeAssertion(WorkingTime.AT_INITIAL, s, metadata=metadata_attr, expected_value=1)
            assert getattr(s, metadata_attr) == test_crawler_name, \
                f"In initialing process, meta data *GroupState.{metadata_attr}* should save value " \
                f"'{_Crawler_Name_Value}'."

        # Operate target method for testing
        init_state = Initial.group_state(crawler_name=_Crawler_Name_Value,
                                         total_crawler=_Total_Crawler_Value,
                                         total_runner=_Runner_Crawler_Value,
                                         total_backup=_Backup_Crawler_Value)
        state = Update.group_state(
            init_state,
            total_crawler=_Total_Crawler_Value + 1,
            total_runner=_Runner_Crawler_Value + 1,
            total_backup=_Backup_Crawler_Value + 1,
            standby_id=test_standby_id,
            append_current_crawler=test_crawler_name,
            append_current_runner=test_crawler_name,
            append_current_backup=test_crawler_name,
            append_fail_crawler=test_crawler_name,
            append_fail_runner=test_crawler_name,
            append_fail_backup=test_crawler_name
        )

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="total_crawler", expected_value=_Total_Crawler_Value + 1)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="total_runner", expected_value=_Runner_Crawler_Value + 1)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="total_backup", expected_value=_Backup_Crawler_Value + 1)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state, metadata="standby_id", expected_value=test_standby_id)

        ListSizeAssertion(WorkingTime.AT_INITIAL, state, metadata="current_crawler", expected_value=2)
        _chk_list_len_and_ele(state, metadata_attr="current_runner")
        _chk_list_len_and_ele(state, metadata_attr="current_backup")

        _chk_list_len_and_ele(state, metadata_attr="fail_crawler")
        _chk_list_len_and_ele(state, metadata_attr="fail_runner")
        _chk_list_len_and_ele(state, metadata_attr="fail_backup")

    def test_node_state(self):
        # Operate target method for testing
        init_state = Initial.node_state()
        state = Update.node_state(init_state, group=_Crawler_Group_Name_Value, role=CrawlerStateRole.RUNNER)

        # Verify values
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, state, is_none=False)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="group", expected_value=_Crawler_Group_Name_Value)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, state,
                               metadata="role", expected_value=CrawlerStateRole.RUNNER.value)

    def test_task(self):
        test_cookie = {"test_cookie": "test_cookie"}
        test_auth = {"test_auth": "test_auth"}

        # Operate target method for testing
        init_task = Initial.task()
        task = Update.task(
            init_task,
            running_content=_Task_Running_Content_Value,
            cookie=test_cookie,
            authorization=test_auth,
            in_progressing_id="1",
            running_result=RunningResult(success_count=1, fail_count=0),
            running_status=TaskResult.PROCESSING,
            result_detail=_Task_Result_Detail_Value
        )

        # Verify values
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task,
                               metadata="running_content", expected_value=_Task_Running_Content_Value)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="cookie", expected_value=test_cookie)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="authorization", expected_value=test_auth)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task, metadata="in_progressing_id", expected_value="1")
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task,
                               metadata="running_result", expected_value={"success_count": 1, "fail_count": 0})
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task,
                               metadata="running_status", expected_value=TaskResult.PROCESSING.value)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, task,
                               metadata="result_detail", expected_value=_Task_Result_Detail_Value)

    def test_heartbeat(self):
        test_datetime_now = datetime.now()
        test_time_format = "%Y/%m/%d %H:%M:%S"
        test_update_time = "5s"
        test_update_timeout = "10s"
        test_heart_rhythm_timeout = "2"

        # Operate target method for testing
        init_heartbeat = Initial.heartbeat()
        heartbeat = Update.heartbeat(
            init_heartbeat,
            heart_rhythm_time=test_datetime_now,
            time_format=test_time_format,
            update_time=test_update_time,
            update_timeout=test_update_timeout,
            heart_rhythm_timeout=test_heart_rhythm_timeout,
            healthy_state=HeartState.HEALTHY,
            task_state=TaskResult.PROCESSING
        )

        # Verify value
        ObjectIsNoneOrNotAssertion(WorkingTime.AT_INITIAL, heartbeat, is_none=False)

        MetaDataValueAssertion(WorkingTime.AT_INITIAL, heartbeat,
                               metadata="heart_rhythm_time",
                               expected_value=test_datetime_now.strftime(test_time_format))
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, heartbeat,
                               metadata="time_format", expected_value=test_time_format)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, heartbeat,
                               metadata="update_time", expected_value=test_update_time)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, heartbeat,
                               metadata="update_timeout", expected_value=test_update_timeout)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, heartbeat,
                               metadata="heart_rhythm_timeout", expected_value=test_heart_rhythm_timeout)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, heartbeat,
                               metadata="healthy_state", expected_value=HeartState.HEALTHY.value)
        MetaDataValueAssertion(WorkingTime.AT_INITIAL, heartbeat,
                               metadata="task_state", expected_value=TaskResult.PROCESSING.value)

