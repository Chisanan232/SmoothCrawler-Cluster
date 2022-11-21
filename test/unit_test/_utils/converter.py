from smoothcrawler_cluster.model.metadata import GroupState, NodeState, Task, RunningContent, RunningResult, ResultDetail, Heartbeat
from smoothcrawler_cluster._utils.converter import JsonStrConverter, TaskContentDataUtils
import pytest
import json

from ..._values import (
    _Test_Group_State_Data, _Test_Node_State_Data, _Test_Task_Data, _Test_Heartbeat_Data,
    _Task_Running_Content_Value, _Task_Running_Result, _Task_Result_Detail_Value,
    setup_group_state, setup_node_state, setup_task, setup_heartbeat
)


class TestJsonStrConverter:

    @pytest.fixture(scope="function")
    def converter(self) -> JsonStrConverter:
        return JsonStrConverter()

    def test__convert_to_str(self, converter: JsonStrConverter):
        # Initial process
        _state = setup_group_state()

        # Run target testing function
        _serialized_data = converter._convert_to_str(data=_state.to_readable_object())

        # Verify running result
        self._verify_serialize_result(serialized_data=_serialized_data, expected_data=json.dumps(_Test_Group_State_Data))

    def test__convert_from_str(self, converter: JsonStrConverter):
        # Run target testing function
        _deserialized_data: dict = converter._convert_from_str(data=json.dumps(_Test_Group_State_Data))

        # Verify running result
        self._verify_deserialize_result(deserialized_data=_deserialized_data, expected_data=_Test_Group_State_Data)

    def test_serialize_meta_data(self, converter: JsonStrConverter):
        # Initial process
        _group_state = setup_group_state()
        _node_state = setup_node_state()
        _task = setup_task()
        _heartbeat = setup_heartbeat()

        # Run target testing function
        _group_state_str = converter.serialize_meta_data(obj=_group_state)
        _node_state_str = converter.serialize_meta_data(obj=_node_state)
        _task_str = converter.serialize_meta_data(obj=_task)
        _heartbeat_str = converter.serialize_meta_data(obj=_heartbeat)

        # Verify running result
        self._verify_serialize_result(serialized_data=_group_state_str, expected_data=json.dumps(_group_state.to_readable_object()))
        self._verify_serialize_result(serialized_data=_node_state_str, expected_data=json.dumps(_node_state.to_readable_object()))
        self._verify_serialize_result(serialized_data=_task_str, expected_data=json.dumps(_task.to_readable_object()))
        self._verify_serialize_result(serialized_data=_heartbeat_str, expected_data=json.dumps(_heartbeat.to_readable_object()))

    def test_deserialize_meta_data(self, converter: JsonStrConverter):
        # Initial process
        _test_group_state_str = json.dumps(_Test_Group_State_Data)
        _test_node_state_str = json.dumps(_Test_Node_State_Data)
        _test_task_str = json.dumps(_Test_Task_Data)
        _test_heartbeat_str = json.dumps(_Test_Heartbeat_Data)

        # Run target testing function
        _group_state = converter.deserialize_meta_data(data=_test_group_state_str, as_obj=GroupState)
        _node_state = converter.deserialize_meta_data(data=_test_node_state_str, as_obj=NodeState)
        _task = converter.deserialize_meta_data(data=_test_task_str, as_obj=Task)
        _heartbeat = converter.deserialize_meta_data(data=_test_heartbeat_str, as_obj=Heartbeat)

        # Verify running result
        self._verify_deserialize_result(deserialized_data=_group_state.to_readable_object(), expected_data=_Test_Group_State_Data)
        self._verify_deserialize_result(deserialized_data=_node_state.to_readable_object(), expected_data=_Test_Node_State_Data)
        self._verify_deserialize_result(deserialized_data=_task.to_readable_object(), expected_data=_Test_Task_Data)
        self._verify_deserialize_result(deserialized_data=_heartbeat.to_readable_object(), expected_data=_Test_Heartbeat_Data)

    @classmethod
    def _verify_serialize_result(cls, serialized_data: str, expected_data: str) -> None:
        _serialized_data_dict: dict = json.loads(serialized_data)
        _expected_data_dict: dict = json.loads(expected_data)
        assert _serialized_data_dict.keys() == _expected_data_dict.keys(), \
            "The keys of both dict objects *_serialized_data_dict* and *_expected_data_dict* should be the same."
        for _key in _expected_data_dict.keys():
            assert _serialized_data_dict[_key] == _expected_data_dict[_key], \
                f"The values of both objects *_serialized_data_dict* and *_expected_data_dict* with key '{_key}' should be the same."

    @classmethod
    def _verify_deserialize_result(cls, deserialized_data: dict, expected_data: dict) -> None:
        assert expected_data.keys() == deserialized_data.keys(), \
            "The keys of both dict objects *deserialized_data* and *expected_data* should be the same."
        for _key in expected_data.keys():
            assert deserialized_data[_key] == expected_data[_key], \
                f"The values of both objects *deserialized_data* and *expected_data* with key '{_key}' should be the same."


class TestTaskContentDataUtils:

    @pytest.fixture(scope="function")
    def utils(self) -> TaskContentDataUtils:
        return TaskContentDataUtils()

    def test_convert_to_running_content(self, utils: TaskContentDataUtils):
        _running_content = utils.convert_to_running_content(data=_Task_Running_Content_Value[0])
        assert type(_running_content) is RunningContent, ""
        assert _running_content.task_id == _Task_Running_Content_Value[0]["task_id"], "The task ID values should be the same."
        assert _running_content.url == _Task_Running_Content_Value[0]["url"], ""
        assert _running_content.method == _Task_Running_Content_Value[0]["method"], ""
        assert _running_content.parameters == _Task_Running_Content_Value[0]["parameters"], ""
        assert _running_content.header == _Task_Running_Content_Value[0]["header"], ""
        assert _running_content.body == _Task_Running_Content_Value[0]["body"], ""

    def test_convert_to_running_result(self, utils: TaskContentDataUtils):
        _running_result = utils.convert_to_running_result(data=_Task_Running_Result)
        assert type(_running_result) is RunningResult, ""
        assert _running_result.success_count == _Task_Running_Result["success_count"], ""
        assert _running_result.fail_count == _Task_Running_Result["fail_count"], ""

    def test_convert_to_result_detail(self, utils: TaskContentDataUtils):
        _result_detail = utils.convert_to_result_detail(data=_Task_Result_Detail_Value[0])
        assert type(_result_detail) is ResultDetail, ""
        assert _result_detail.task_id == _Task_Result_Detail_Value[0]["task_id"], ""
        assert _result_detail.state == _Task_Result_Detail_Value[0]["state"], ""
        assert _result_detail.status_code == _Task_Result_Detail_Value[0]["status_code"], ""
        assert _result_detail.response == _Task_Result_Detail_Value[0]["response"], ""
        assert _result_detail.error_msg == _Task_Result_Detail_Value[0]["error_msg"], ""

