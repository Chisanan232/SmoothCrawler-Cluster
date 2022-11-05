from smoothcrawler_cluster._utils.converter import JsonStrConverter
from smoothcrawler_cluster.model.metadata import CrawlerStateRole, TaskResult, State, Task,  Heartbeat
import pytest
import json


_Test_State_Data = {
    "role": CrawlerStateRole.Runner.value,
    "total_crawler": 3,
    "total_runner": 2,
    "total_backup": 1,
    "standby_id": "3",
    "current_crawler": ["spider_1", "spider_2", "spider_3"],
    "current_runner": ["spider_1", "spider_2"],
    "current_backup": ["spider_3"],
    "fail_crawler": [],
    "fail_runner": [],
    "fail_backup": []
}

_Test_Task_Data = {
    "task_content": {
        "url": "xxx",
        "method": "GET",
        "parameter": "",
        "body": ""
    },
    "task_result": TaskResult.Done.value
}

_Test_Heartbeat_Data = {
    "datetime": "2022-07-15 08:42:59"
}


def setup_state() -> State:
    _state = State()
    _state.role = _Test_State_Data["role"]
    _state.total_crawler = _Test_State_Data["total_crawler"]
    _state.total_runner = _Test_State_Data["total_runner"]
    _state.total_backup = _Test_State_Data["total_backup"]
    _state.current_crawler = _Test_State_Data["current_crawler"]
    _state.current_runner = _Test_State_Data["current_runner"]
    _state.current_backup = _Test_State_Data["current_backup"]
    _state.fail_crawler = _Test_State_Data["fail_crawler"]
    _state.fail_runner = _Test_State_Data["fail_runner"]
    _state.fail_backup = _Test_State_Data["fail_backup"]
    _state.standby_id = _Test_State_Data["standby_id"]
    return _state


def setup_task() -> Task:
    _task = Task()
    _task.task_content = _Test_Task_Data["task_content"]
    _task.task_result = _Test_Task_Data["task_result"]
    return _task


def setup_heartbeat() -> Heartbeat:
    _heartbeat = Heartbeat()
    _heartbeat.heart_rhythm_time = _Test_Heartbeat_Data["datetime"]
    return _heartbeat


class TestJsonStrConverter:

    @pytest.fixture(scope="function")
    def converter(self) -> JsonStrConverter:
        return JsonStrConverter()

    def test_serialize(self, converter: JsonStrConverter):
        # Initial process
        _state = setup_state()

        # Run target testing function
        _serialized_data = converter.serialize(data=_state.to_readable_object())

        # Verify running result
        self._verify_serialize_result(serialized_data=_serialized_data, expected_data=json.dumps(_Test_State_Data))

    def test_deserialize(self, converter: JsonStrConverter):
        # Run target testing function
        _deserialized_data: dict = converter.deserialize(data=json.dumps(_Test_State_Data))

        # Verify running result
        self._verify_deserialize_result(deserialized_data=_deserialized_data, expected_data=_Test_State_Data)

    def test_state_to_str(self, converter: JsonStrConverter):
        # Initial process
        _state = setup_state()

        # Run target testing function
        _state_str = converter.state_to_str(state=_state)

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

    def test_str_to_state(self, converter: JsonStrConverter):
        # Initial process
        _test_state_str = json.dumps(_Test_State_Data)

        # Run target testing function
        _state = converter.str_to_state(data=_test_state_str)

        # Verify running result
        self._verify_deserialize_result(deserialized_data=_state.to_readable_object(), expected_data=_Test_State_Data)

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
