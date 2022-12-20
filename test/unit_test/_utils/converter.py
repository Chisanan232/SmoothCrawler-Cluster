from smoothcrawler_cluster.model.metadata import (
    GroupState, NodeState, Task, RunningContent, RunningResult, ResultDetail, Heartbeat)
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
        state = setup_group_state()

        # Run target testing function
        serialized_data = converter._convert_to_str(data=state.to_readable_object())

        # Verify running result
        self._verify_serialize_result(serialized_data=serialized_data, expected_data=json.dumps(_Test_Group_State_Data))

    def test__convert_from_str(self, converter: JsonStrConverter):
        # Run target testing function
        deserialized_data: dict = converter._convert_from_str(data=json.dumps(_Test_Group_State_Data))

        # Verify running result
        self._verify_deserialize_result(deserialized_data=deserialized_data, expected_data=_Test_Group_State_Data)

    def test_serialize_meta_data(self, converter: JsonStrConverter):
        # Initial process
        group_state = setup_group_state()
        node_state = setup_node_state()
        task = setup_task()
        heartbeat = setup_heartbeat()

        # Run target testing function
        group_state_str = converter.serialize_meta_data(obj=group_state)
        node_state_str = converter.serialize_meta_data(obj=node_state)
        task_str = converter.serialize_meta_data(obj=task)
        heartbeat_str = converter.serialize_meta_data(obj=heartbeat)

        # Verify running result
        self._verify_serialize_result(serialized_data=group_state_str,
                                      expected_data=json.dumps(group_state.to_readable_object()))
        self._verify_serialize_result(serialized_data=node_state_str,
                                      expected_data=json.dumps(node_state.to_readable_object()))
        self._verify_serialize_result(serialized_data=task_str,
                                      expected_data=json.dumps(task.to_readable_object()))
        self._verify_serialize_result(serialized_data=heartbeat_str,
                                      expected_data=json.dumps(heartbeat.to_readable_object()))

    def test_deserialize_meta_data(self, converter: JsonStrConverter):
        # Initial process
        test_group_state_str = json.dumps(_Test_Group_State_Data)
        test_node_state_str = json.dumps(_Test_Node_State_Data)
        test_task_str = json.dumps(_Test_Task_Data)
        test_heartbeat_str = json.dumps(_Test_Heartbeat_Data)

        # Run target testing function
        group_state = converter.deserialize_meta_data(data=test_group_state_str, as_obj=GroupState)
        node_state = converter.deserialize_meta_data(data=test_node_state_str, as_obj=NodeState)
        task = converter.deserialize_meta_data(data=test_task_str, as_obj=Task)
        heartbeat = converter.deserialize_meta_data(data=test_heartbeat_str, as_obj=Heartbeat)

        # Verify running result
        self._verify_deserialize_result(deserialized_data=group_state.to_readable_object(),
                                        expected_data=_Test_Group_State_Data)
        self._verify_deserialize_result(deserialized_data=node_state.to_readable_object(),
                                        expected_data=_Test_Node_State_Data)
        self._verify_deserialize_result(deserialized_data=task.to_readable_object(),
                                        expected_data=_Test_Task_Data)
        self._verify_deserialize_result(deserialized_data=heartbeat.to_readable_object(),
                                        expected_data=_Test_Heartbeat_Data)

    @classmethod
    def _verify_serialize_result(cls, serialized_data: str, expected_data: str) -> None:
        serialized_data_dict: dict = json.loads(serialized_data)
        expected_data_dict: dict = json.loads(expected_data)
        assert serialized_data_dict.keys() == expected_data_dict.keys(), \
            "The keys of both dict objects *serialized_data_dict* and *expected_data_dict* should be the same."
        for key in expected_data_dict.keys():
            assert serialized_data_dict[key] == expected_data_dict[key], \
                f"The values of both objects *serialized_data_dict* and *expected_data_dict* with key '{key}' should " \
                f"be the same."

    @classmethod
    def _verify_deserialize_result(cls, deserialized_data: dict, expected_data: dict) -> None:
        assert expected_data.keys() == deserialized_data.keys(), \
            "The keys of both dict objects *deserialized_data* and *expected_data* should be the same."
        for key in expected_data.keys():
            assert deserialized_data[key] == expected_data[key], \
                f"The values of both objects *deserialized_data* and *expected_data* with key '{key}' should be the" \
                f" same."


class TestTaskContentDataUtils:

    @pytest.fixture(scope="function")
    def utils(self) -> TaskContentDataUtils:
        return TaskContentDataUtils()

    def test_convert_to_running_content(self, utils: TaskContentDataUtils):
        running_content = utils.convert_to_running_content(data=_Task_Running_Content_Value[0])
        content = _Task_Running_Content_Value[0]
        assert isinstance(running_content, RunningContent), "It should be converted as 'RunningContent' type object."
        assert running_content.task_id == content["task_id"], "The *task_id* values should be the same."
        assert running_content.url == content["url"], "The *url* values should be the same."
        assert running_content.method == content["method"], "The *method* values should be the same."
        assert running_content.parameters == content["parameters"], "The *parameters* values should be the same."
        assert running_content.header == content["header"], "The *header* values should be the same."
        assert running_content.body == content["body"], "The *body* values should be the same."

    def test_convert_to_running_result(self, utils: TaskContentDataUtils):
        running_result = utils.convert_to_running_result(data=_Task_Running_Result)
        assert isinstance(running_result, RunningResult), "It should be converted as 'RunningResult' type object."
        assert running_result.success_count == _Task_Running_Result["success_count"], \
            "The *success_count* values should be the same."
        assert running_result.fail_count == _Task_Running_Result["fail_count"], \
            "The *fail_count* values should be the same."

    def test_convert_to_result_detail(self, utils: TaskContentDataUtils):
        result_detail = utils.convert_to_result_detail(data=_Task_Result_Detail_Value[0])
        detail = _Task_Result_Detail_Value[0]
        assert isinstance(result_detail, ResultDetail), "It should be converted as 'ResultDetail' type object."
        assert result_detail.task_id == detail["task_id"], "The *task_id* values should be the same."
        assert result_detail.state == detail["state"], "The *state* values should be the same."
        assert result_detail.status_code == detail["status_code"], "The *status_code* values should be the same."
        assert result_detail.response == detail["response"], "The *response* values should be the same."
        assert result_detail.error_msg == detail["error_msg"], "The *error_msg* values should be the same."

