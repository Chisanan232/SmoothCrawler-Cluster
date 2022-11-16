from smoothcrawler_cluster._utils.converter import JsonStrConverter
import pytest
import json

from ..._values import (
    _Test_Group_State_Data, _Test_Node_State_Data, _Test_Task_Data, _Test_Heartbeat_Data,
    setup_group_state, setup_node_state, setup_task, setup_heartbeat
)


class TestJsonStrConverter:

    @pytest.fixture(scope="function")
    def converter(self) -> JsonStrConverter:
        return JsonStrConverter()

    def test_serialize(self, converter: JsonStrConverter):
        # Initial process
        _state = setup_group_state()

        # Run target testing function
        _serialized_data = converter._convert_to_str(data=_state.to_readable_object())

        # Verify running result
        self._verify_serialize_result(serialized_data=_serialized_data, expected_data=json.dumps(_Test_Group_State_Data))

    def test_deserialize(self, converter: JsonStrConverter):
        # Run target testing function
        _deserialized_data: dict = converter._convert_from_str(data=json.dumps(_Test_Group_State_Data))

        # Verify running result
        self._verify_deserialize_result(deserialized_data=_deserialized_data, expected_data=_Test_Group_State_Data)

    def test_group_state_to_str(self, converter: JsonStrConverter):
        # Initial process
        _state = setup_group_state()

        # Run target testing function
        _state_str = converter.group_state_to_str(state=_state)

        # Verify running result
        self._verify_serialize_result(serialized_data=_state_str, expected_data=json.dumps(_state.to_readable_object()))

    def test_node_state_to_str(self, converter: JsonStrConverter):
        # Initial process
        _state = setup_node_state()

        # Run target testing function
        _state_str = converter.node_state_to_str(state=_state)

        # Verify running result
        self._verify_serialize_result(serialized_data=_state_str, expected_data=json.dumps(_state.to_readable_object()))

    def test_task_to_str(self, converter: JsonStrConverter):
        # Initial process
        _task = setup_task()

        # Run target testing function
        _task_str = converter.task_to_str(task=_task)

        # Verify running result
        self._verify_serialize_result(serialized_data=_task_str, expected_data=json.dumps(_task.to_readable_object()))

    def test_heartbeat_to_str(self, converter: JsonStrConverter):
        # Initial process
        _heartbeat = setup_heartbeat()

        # Run target testing function
        _heartbeat_str = converter.heartbeat_to_str(heartbeat=_heartbeat)

        # Verify running result
        self._verify_serialize_result(serialized_data=_heartbeat_str, expected_data=json.dumps(_heartbeat.to_readable_object()))

    def test_str_to_group_state(self, converter: JsonStrConverter):
        # Initial process
        _test_state_str = json.dumps(_Test_Group_State_Data)

        # Run target testing function
        _state = converter.str_to_group_state(data=_test_state_str)

        # Verify running result
        self._verify_deserialize_result(deserialized_data=_state.to_readable_object(), expected_data=_Test_Group_State_Data)

    def test_str_to_node_state(self, converter: JsonStrConverter):
        # Initial process
        _test_state_str = json.dumps(_Test_Node_State_Data)

        # Run target testing function
        _state = converter.str_to_node_state(data=_test_state_str)

        # Verify running result
        self._verify_deserialize_result(deserialized_data=_state.to_readable_object(), expected_data=_Test_Node_State_Data)

    def test_str_to_task(self, converter: JsonStrConverter):
        # Initial process
        _test_task_str = json.dumps(_Test_Task_Data)

        # Run target testing function
        _task = converter.str_to_task(data=_test_task_str)

        # Verify running result
        self._verify_deserialize_result(deserialized_data=_task.to_readable_object(), expected_data=_Test_Task_Data)

    def test_str_to_heartbeat(self, converter: JsonStrConverter):
        # Initial process
        _test_heartbeat_str = json.dumps(_Test_Heartbeat_Data)

        # Run target testing function
        _heartbeat = converter.str_to_heartbeat(data=_test_heartbeat_str)

        # Verify running result
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
