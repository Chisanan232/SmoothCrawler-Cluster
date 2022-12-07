from smoothcrawler_cluster.model import TaskResult, GroupState, NodeState, Task, Heartbeat
from kazoo.client import KazooClient
from datetime import datetime
from typing import Dict, Type, Union
import json
import re

from .integration_test._test_utils._instance_value import _TestValue, _ZKNodePathUtils
from ._metadata_values import (
    GroupStateByObject, GroupStateByJsonData,
    NodeStateByObject, NodeStateByJsonData,
    TaskDataFromObject, TaskDataFromJsonData,
    HeartbeatFromObject, HeartbeatFromJsonData
)
from ._values import _Task_Running_Content_Value


_Testing_Value: _TestValue = _TestValue()


class Verify:

    def __init__(self):
        self._client = None

    def initial_zk_session(self, client: KazooClient):
        self._client = client

    def group_state_info(self, runner: int, backup: int, fail_runner: int, review_data: Union[str, bytes, GroupState] = None):
        _group_state = self.__get_metadata_opts(
            review_data,
            metadata_type=GroupState,
            zk_path=_Testing_Value.group_state_zookeeper_path,
            data_by_object=GroupStateByObject,
            data_by_json_obj=GroupStateByJsonData
        )

        assert _group_state.total_crawler == runner + backup, ""
        assert _group_state.total_runner == runner, ""
        assert _group_state.total_backup == backup - fail_runner, ""

        assert len(_group_state.current_crawler) == runner + backup - fail_runner, ""
        # TODO: How to let fail crawler name to be parameter?
        assert False not in ["2" not in _crawler for _crawler in _group_state.current_crawler], ""
        assert len(_group_state.current_runner) == runner, ""
        assert False not in ["2" not in _crawler for _crawler in _group_state.current_runner], ""
        assert len(_group_state.current_backup) == backup - fail_runner, ""

        assert _group_state.standby_id == "4", ""

        assert len(_group_state.fail_crawler) == fail_runner, ""
        assert False not in ["2" in _crawler for _crawler in _group_state.fail_crawler], ""
        assert len(_group_state.fail_runner) == fail_runner, ""
        assert False not in ["2" in _crawler for _crawler in _group_state.fail_runner], ""
        assert len(_group_state.fail_backup) == 0, ""

    def all_node_state_role(self, runner: int, backup: int, expected_role: dict, expected_group: dict):
        _state_paths = _ZKNodePathUtils.all_node_state(runner + backup)
        for _state_path in list(_state_paths):
            _data, _state = self._client.get(path=_state_path)
            _json_data = json.loads(_data.decode("utf-8"))
            print(f"[DEBUG in testing] _state_path: {_state_path}, _json_data: {_json_data} - {datetime.now()}")
            _chksum = re.search(r"[0-9]{1,3}", _state_path)
            if _chksum is not None:
                assert _json_data["role"] == expected_role[_chksum.group(0)].value, ""
                assert _json_data["group"] == expected_group[_chksum.group(0)], ""
            else:
                assert False, ""

    def all_task_detail(self, runner: int, backup: int, expected_task_result: dict):
        _task_paths = _ZKNodePathUtils.all_task(runner + backup)
        for _task_path in list(_task_paths):
            _data, _state = self._client.get(path=_task_path)
            _json_data = json.loads(_data.decode("utf-8"))
            print(f"[DEBUG in testing] _task_path: {_task_path}, _json_data: {_json_data}")
            assert _json_data is not None, ""
            _chksum = re.search(r"[0-9]{1,3}", _task_path)
            if _chksum is not None:
                _chk_detail_assert = getattr(self, f"_chk_{expected_task_result[_chksum.group(0)]}_task_detail")
                _chk_detail_assert(_json_data)
            else:
                assert False, ""

    @classmethod
    def _check_running_status(cls, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread gets any exception in running."

    @classmethod
    def _chk_available_task_detail(cls, _one_detail: dict) -> None:
        assert len(_one_detail["running_content"]) == 0, ""
        assert _one_detail["in_progressing_id"] == "-1", ""
        assert _one_detail["running_result"] == {"success_count": 1, "fail_count": 0}, ""
        assert _one_detail["running_status"] == TaskResult.Done.value, ""
        assert _one_detail["result_detail"][0] == {
            "task_id": _Task_Running_Content_Value[0]["task_id"],
            "state": TaskResult.Done.value,
            "status_code": 200,
            "response": "Example Domain",
            "error_msg": None
        }, "The detail should be completely same as above."

    @classmethod
    def _chk_nothing_task_detail(cls, _one_detail: dict) -> None:
        assert len(_one_detail["running_content"]) == 0, ""
        assert _one_detail["in_progressing_id"] == "0", ""
        assert _one_detail["running_result"] == {"success_count": 0, "fail_count": 0}, ""
        assert _one_detail["running_status"] == TaskResult.Nothing.value, ""
        assert len(_one_detail["result_detail"]) == 0, "It should NOT have any running result because it is backup role."

    @classmethod
    def _chk_processing_task_detail(cls, _one_detail: dict) -> None:
        assert len(_one_detail["running_content"]) == 1, ""
        assert _one_detail["in_progressing_id"] == "0", ""
        assert _one_detail["running_result"] == {"success_count": 0, "fail_count": 0}, ""
        assert _one_detail["running_status"] == TaskResult.Processing.value, ""
        assert len(_one_detail["result_detail"]) == 0, "It should NOT have any running result because it is backup role."

    @classmethod
    def _chk_backup_task_detail(cls, _one_detail: dict) -> None:
        assert len(_one_detail["running_content"]) == 0, ""
        assert _one_detail["in_progressing_id"] == "-1", ""
        assert _one_detail["running_result"] == {"success_count": 0, "fail_count": 0}, ""
        assert _one_detail["running_status"] == TaskResult.Nothing.value, ""
        assert len(_one_detail["result_detail"]) == 0, "It should NOT have any running result because it is backup role."

    def __get_metadata_opts(self, review_data: Union[str, bytes, GroupState, NodeState, Task, Heartbeat],
                            zk_path: str, metadata_type: Type[Union[GroupState, NodeState, Task, Heartbeat]],
                            data_by_json_obj: Type[Union[GroupStateByJsonData, NodeStateByJsonData, TaskDataFromJsonData, HeartbeatFromJsonData]],
                            data_by_object: Type[Union[GroupStateByObject, NodeStateByObject, TaskDataFromObject, HeartbeatFromObject]]):
        if review_data is None:
            _data, _state = self._client.get(path=zk_path)
            assert len(_data) != 0, "The data content of meta data *GroupState* should NOT be empty."
            _meta_data_opt = data_by_json_obj(data=_data)
            print(f"[DEBUG in testing] path: {zk_path}, json_data: {_meta_data_opt}")
        else:
            if type(review_data) is metadata_type:
                _meta_data_opt = data_by_object(data=review_data)
            elif type(review_data) in [str, bytes]:
                _meta_data_opt = data_by_json_obj(data=review_data)
            else:
                raise TypeError(f"Doesn't support data type {type(review_data)} processing.")
        return _meta_data_opt
