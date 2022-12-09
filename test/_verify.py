from smoothcrawler_cluster.model import TaskResult, HeartState, GroupState, NodeState, Task, Heartbeat
from multiprocessing.managers import DictProxy
from kazoo.client import KazooClient
from datetime import datetime
from typing import Dict, Type, Union
import traceback
import re

from .integration_test._test_utils._instance_value import _TestValue, _ZKNodePathUtils
from ._metadata_values import (
    GroupStateData, GroupStateByObject, GroupStateByJsonData,
    NodeStateData, NodeStateByObject, NodeStateByJsonData,
    TaskData, TaskDataFromObject, TaskDataFromJsonData,
    HeartbeatData, HeartbeatFromObject, HeartbeatFromJsonData
)
from ._values import _Task_Running_Content_Value, _Time_Format_Value


_Testing_Value: _TestValue = _TestValue()


def _equal_assertion(under_test, expect) -> None:
    assert under_test == expect, f"The value should be the same. Under test: {under_test}, expected value: {expect}."


class Verify:

    @classmethod
    def exception(cls, exception: Union[Exception, Dict[str, Exception]]) -> None:

        def _print_traced_exception(_e_name: str, _e: Exception) -> str:
            print(f"[DEBUG in Verify] _e_name: {_e_name}")
            print(f"[DEBUG in Verify] _e: {_e}")
            return f"=========================== {_e_name} ===========================" \
                   f"{traceback.format_exception(_e)}" \
                   f"================================================================="

        print(f"[DEBUG in Verify] exception: {exception}")
        if type(exception) in [dict, DictProxy]:
            _has_except = list(filter(lambda e: e is not None, exception.values()))
            if len(_has_except) == 0:
                assert True, "It doesn't have any exception in any thread or process."
            elif len(_has_except) == 1:
                raise _has_except[0]
            else:
                assert False, map(_print_traced_exception, exception.items())
        else:
            if exception is not None:
                assert False, traceback.print_exception(exception)

    @classmethod
    def running_status(cls, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread or process gets any exception in running."


class VerifyMetaData:

    def __init__(self):
        self._client = None

    def initial_zk_session(self, client: KazooClient) -> None:
        self._client = client

    def group_state_is_not_empty(self, runner: int, backup: int, standby_id: str, review_data: Union[str, bytes, GroupState] = None) -> None:
        _group_state = self.__get_metadata_opts(
            review_data,
            metadata_type=GroupState,
            zk_path=_Testing_Value.group_state_zookeeper_path,
            data_by_object=GroupStateByObject,
            data_by_json_obj=GroupStateByJsonData
        )

        _equal_assertion(_group_state.total_crawler, runner + backup)
        _equal_assertion(_group_state.total_runner, runner)
        _equal_assertion(_group_state.total_backup, backup)
        _equal_assertion(_group_state.standby_id, standby_id)

    def group_state_info(self, runner: int, backup: int, fail_runner: int = 0, fail_runner_name: str = None, standby_id: str = None,
                         index_sep_char: str = "_", review_data: Union[str, bytes, GroupState] = None) -> None:
        _group_state = self.__get_metadata_opts(
            review_data,
            metadata_type=GroupState,
            zk_path=_Testing_Value.group_state_zookeeper_path,
            data_by_object=GroupStateByObject,
            data_by_json_obj=GroupStateByJsonData
        )

        # Verify total parts
        assert _group_state.total_crawler == runner + backup, f"The attribute 'total_crawler' should be '{runner + backup}'."
        assert _group_state.total_runner == runner, f"The attribute 'total_runner' should be '{runner}'."
        assert _group_state.total_backup == backup - fail_runner, f"The attribute 'total_backup' should be '{backup - fail_runner}'."

        # # Verify current section
        self.group_state_current_section(review_data=review_data, runner=runner, backup=backup, fail_runner=fail_runner, index_sep_char=index_sep_char)

        # Verify standby_id
        if standby_id is not None:
            assert _group_state.standby_id == standby_id, f"The attribute 'standby_id' should be '{standby_id}'."
        _standby_id_checksum_list = list(map(lambda _crawler: _group_state.standby_id in _crawler, _group_state.current_backup))
        if len(_group_state.current_backup) != 0:
            assert True in _standby_id_checksum_list, "The standby ID (the index of crawler name) should be included in the one name of backup crawlers list."

        # Verify fail parts
        assert len(_group_state.fail_crawler) == fail_runner, f"The length of attribute 'fail_crawler' should be '{fail_runner}'."
        if fail_runner != 0 and fail_runner_name is not None:
            assert False not in [fail_runner_name in _crawler for _crawler in _group_state.fail_crawler], ""
        assert len(_group_state.fail_runner) == fail_runner, f"The length of attribute 'fail_runner' should be '{fail_runner}'."
        if fail_runner != 0 and fail_runner_name is not None:
            assert False not in [fail_runner_name in _crawler for _crawler in _group_state.fail_runner], ""
        assert len(_group_state.fail_backup) == 0, f"The length of attribute 'fail_backup' should be '{0}'."

    def group_state_current_section(self, runner: int, backup: int, fail_runner: int = 0, fail_runner_name: str = None,
                                    index_sep_char: str = "_", review_data: Union[str, bytes, GroupState, GroupStateData] = None,
                                    verify_crawler: bool = True, verify_runner: bool = True, verify_backup: bool = True) -> None:
        if type(review_data) is GroupStateData:
            _group_state = review_data
        else:
            _group_state = self.__get_metadata_opts(
                review_data,
                metadata_type=GroupState,
                zk_path=_Testing_Value.group_state_zookeeper_path,
                data_by_object=GroupStateByObject,
                data_by_json_obj=GroupStateByJsonData
            )

        # Verify current_crawler
        if verify_crawler is True:
            assert len(_group_state.current_crawler) == runner + backup - fail_runner, \
                f"The length of attribute 'current_crawler' should be '{runner + backup - fail_runner}'."
            assert len(_group_state.current_crawler) == len(set(_group_state.current_crawler)), \
                "Attribute *current_crawler* should NOT have any element is repeated."
            # TODO: How to let fail crawler name to be parameter?
            if fail_runner != 0 and fail_runner_name is not None:
                assert False not in [fail_runner_name not in _crawler for _crawler in _group_state.current_crawler], ""

        # Verify current_runner
        if verify_runner is True:
            assert len(_group_state.current_runner) == runner, f"The length of attribute 'current_runner' should be '{runner}'."
            if fail_runner == 0:
                _runer_checksum_iter = map(lambda _crawler: int(_crawler.split(index_sep_char)[-1]) <= runner, _group_state.current_runner)
                assert False not in list(_runer_checksum_iter), f"The index of all crawler name should be <= {runner} (the count of runner)."
            if fail_runner != 0 and fail_runner_name is not None:
                assert False not in [fail_runner_name not in _crawler for _crawler in _group_state.current_runner], ""

        # Verify current_backup
        if verify_backup is True:
            assert len(_group_state.current_backup) == backup - fail_runner, f"The length of attribute 'current_backup' should be '{backup - fail_runner}'."
            if fail_runner == 0:
                _backup_checksum_iter = map(lambda _crawler: int(_crawler.split(index_sep_char)[-1]) > backup, _group_state.current_backup)
                assert False not in list(_backup_checksum_iter), f"The index of all crawler name should be > {backup} (the count of runner)."

    def node_state_is_not_empty(self, role: str, group: str, review_data: Union[str, bytes, GroupState] = None) -> None:
        _node_state = self.__get_metadata_opts(
            review_data,
            metadata_type=NodeState,
            zk_path=_Testing_Value.node_state_zookeeper_path,
            data_by_object=NodeStateByObject,
            data_by_json_obj=NodeStateByJsonData
        )

        _equal_assertion(_node_state.role, role)
        _equal_assertion(_node_state.group, group)

    def all_node_state_role(self, runner: int, backup: int, expected_role: dict, expected_group: dict, start_index: int = 1) -> None:
        _state_paths = _ZKNodePathUtils.all_node_state(size=runner + backup, start_index=start_index)
        for _state_path in list(_state_paths):
            _data, _state = self._client.get(path=_state_path)
            _chksum = re.search(r"[0-9]{1,3}", _state_path)
            if _chksum is not None:
                self.one_node_state(
                    _data,
                    role=expected_role[_chksum.group(0)].value,
                    group=expected_group[_chksum.group(0)]
                )
            else:
                assert False, ""

    def one_node_state(self, node_state: Union[str, bytes, NodeStateData], role: str = None, group: str = None) -> None:
        _node_state = self.__get_metadata_opts(
            node_state,
            metadata_type=NodeState,
            zk_path=_Testing_Value.node_state_zookeeper_path,
            data_by_object=NodeStateByObject,
            data_by_json_obj=NodeStateByJsonData
        )

        if role is not None:
            assert _node_state.role == role, f"The attribute 'role' should be '{role}'."
        if group is not None:
            assert _node_state.group == group, f"The attribute 'group' should be '{group}'."

    def task_is_not_empty(self, running_content: list = [], cookies: dict = {}, authorization: dict = {}, running_result: dict = None,
                          running_status: str = None, review_data: Union[str, bytes, GroupState] = None, result_detail: list = []) -> None:
        _task = self.__get_metadata_opts(
            review_data,
            metadata_type=Task,
            zk_path=_Testing_Value.task_zookeeper_path,
            data_by_object=TaskDataFromObject,
            data_by_json_obj=TaskDataFromJsonData
        )

        _equal_assertion(_task.running_content, running_content)
        _equal_assertion(_task.cookies, cookies)
        _equal_assertion(_task.authorization, authorization)
        if running_status is None:
            running_status = TaskResult.Nothing.value
        _equal_assertion(_task.running_status, running_status)
        if running_result is None:
            running_result = {"success_count": 0, "fail_count": 0}
        _equal_assertion(_task.running_result, running_result)
        _equal_assertion(_task.result_detail, result_detail)

    def all_task_info(self, runner: int, backup: int, running_content_len: int = None, cookies: dict = None,
                      authorization: dict = None, in_progressing_id: str = None, running_status: str = None,
                      running_result: dict = None, result_detail_len: int = None, start_index: int = 1) -> None:
        _task_paths = _ZKNodePathUtils.all_task(size=runner + backup, start_index=start_index)
        for _task_path in list(_task_paths):
            _data, _state = self._client.get(path=_task_path)
            print(f"[DEBUG in testing] _task_path: {_task_path}, _task: {_data}")
            self.one_task_info(
                _data,
                running_content_len=running_content_len,
                cookies=cookies,
                authorization=authorization,
                in_progressing_id=in_progressing_id,
                running_status=running_status,
                running_result=running_result,
                result_detail_len=result_detail_len
            )

    def one_task_info(self, task: Union[str, bytes, Task], running_content_len: int = None, cookies: dict = None,
                      authorization: dict = None, in_progressing_id: str = None, running_status: str = None,
                      running_result: dict = None, result_detail_len: int = None) -> None:
        _task = self.__get_metadata_opts(
            task,
            metadata_type=Task,
            zk_path=_Testing_Value.task_zookeeper_path,
            data_by_object=TaskDataFromObject,
            data_by_json_obj=TaskDataFromJsonData
        )

        assert type(_task.running_content) is list, "The data type of attribute 'running_content' should be list."
        if running_content_len is not None:
            assert len(_task.running_content) == running_content_len, f"The length of attribute 'running_content' should be {len(_task.running_content)}."
        if cookies is not None:
            assert _task.cookies == cookies, f"The attribute 'cookie' should be '{cookies}'."
        if authorization is not None:
            assert _task.authorization == authorization, f"The attribute 'authorization' should be '{authorization}'."
        if in_progressing_id is not None:
            assert _task.in_progressing_id == in_progressing_id, f"The attribute 'in_progressing_id' should be '{in_progressing_id}'."
        if running_status is not None:
            assert _task.running_status == running_status, f"The attribute 'running_status' should be '{running_status}'."
        if running_result is not None:
            assert ("success_count", "fail_count") == tuple(_task.running_result.keys()), \
                "The keys of attribute 'running_result' should be 'success_count' and 'fail_count'."
            assert _task.running_result == running_result, f"The attribute 'running_result' should be '{running_result}'."
        if result_detail_len is not None:
            assert len(_task.result_detail) == result_detail_len, f"The authorization should be '{authorization}'."

    def all_task_detail(self, runner: int, backup: int, expected_task_result: dict, start_index: int = 1) -> None:
        _task_paths = _ZKNodePathUtils.all_task(size=runner + backup, start_index=start_index)
        for _task_path in list(_task_paths):
            _data, _state = self._client.get(path=_task_path)
            print(f"[DEBUG in testing] _task_path: {_task_path}, _task: {_data}")
            self.one_task_result_detail(_data, task_path=_task_path, expected_task_result=expected_task_result)

    def one_task_result_detail(self, task: Union[str, bytes, Task], task_path: str, expected_task_result: dict) -> None:
        _task = self.__get_metadata_opts(
            task,
            metadata_type=Task,
            zk_path=_Testing_Value.task_zookeeper_path,
            data_by_object=TaskDataFromObject,
            data_by_json_obj=TaskDataFromJsonData
        )

        assert _task is not None, ""
        _chksum = re.search(r"[0-9]{1,3}", task_path)
        if _chksum is not None:
            _chk_detail_assert = getattr(self, f"_chk_{expected_task_result[_chksum.group(0)]}_task_detail")
            _chk_detail_assert(_task)
        else:
            assert False, ""

    def all_heartbeat_info(self, runner: int, backup: int, start_index: int = 1) -> None:
        _heartbeat_paths = _ZKNodePathUtils.all_heartbeat(size=runner + backup, start_index=start_index)
        for _path in _heartbeat_paths:
            _heartbeat, _state = self._client.get(path=_path)
            print(f"[DEBUG] _path: {_path}, _task: {_heartbeat}")
            self.one_heartbeat(_heartbeat)

    def one_heartbeat(self, heartbeat: Union[str, bytes, Heartbeat]) -> None:
        _heartbeat = self.__get_metadata_opts(
            heartbeat,
            metadata_type=Heartbeat,
            zk_path=_Testing_Value.heartbeat_zookeeper_path,
            data_by_object=HeartbeatFromObject,
            data_by_json_obj=HeartbeatFromJsonData
        )

        assert _heartbeat.heart_rhythm_time is not None and _heartbeat.heart_rhythm_time != "", ""
        assert _heartbeat.time_format == _Time_Format_Value, ""
        assert re.search(r"[0-9]{1,2}[smh]", _heartbeat.update_time) is not None, ""
        assert re.search(r"[0-9]{1,2}[smh]", _heartbeat.update_timeout) is not None, ""
        assert re.search(r"[0-9]{1,2}", _heartbeat.heart_rhythm_timeout) is not None, ""
        _heart_rhythm_datetime = datetime.strptime(_heartbeat.heart_rhythm_time, _heartbeat.time_format)
        _now_datetime = datetime.now()
        _diff_datetime = _now_datetime - _heart_rhythm_datetime
        assert _diff_datetime.total_seconds() < float(_heartbeat.update_time[:-1]) + float(_heartbeat.update_timeout[:-1]), ""
        assert _heartbeat.healthy_state == HeartState.Healthy.value, ""
        assert _heartbeat.task_state == TaskResult.Nothing.value, ""

    @classmethod
    def _check_running_status(cls, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread gets any exception in running."

    @classmethod
    def _chk_available_task_detail(cls, _one_detail: TaskData) -> None:
        assert len(_one_detail.running_content) == 0, ""
        assert _one_detail.in_progressing_id == "-1", ""
        assert _one_detail.running_result == {"success_count": 1, "fail_count": 0}, ""
        assert _one_detail.running_status == TaskResult.Done.value, ""
        assert _one_detail.result_detail[0] == {
            "task_id": _Task_Running_Content_Value[0]["task_id"],
            "state": TaskResult.Done.value,
            "status_code": 200,
            "response": "Example Domain",
            "error_msg": None
        }, "The detail should be completely same as above."

    @classmethod
    def _chk_nothing_task_detail(cls, _one_detail: TaskData) -> None:
        assert len(_one_detail.running_content) == 0, ""
        assert _one_detail.in_progressing_id == "0", ""
        assert _one_detail.running_result == {"success_count": 0, "fail_count": 0}, ""
        assert _one_detail.running_status == TaskResult.Nothing.value, ""
        assert len(_one_detail.result_detail) == 0, "It should NOT have any running result because it is backup role."

    @classmethod
    def _chk_processing_task_detail(cls, _one_detail: TaskData) -> None:
        assert len(_one_detail.running_content) == 1, ""
        assert _one_detail.in_progressing_id == "0", ""
        assert _one_detail.running_result == {"success_count": 0, "fail_count": 0}, ""
        assert _one_detail.running_status == TaskResult.Processing.value, ""
        assert len(_one_detail.result_detail) == 0, "It should NOT have any running result because it is backup role."

    @classmethod
    def _chk_backup_task_detail(cls, _one_detail: TaskData) -> None:
        assert len(_one_detail.running_content) == 0, ""
        assert _one_detail.in_progressing_id == "-1", ""
        assert _one_detail.running_result == {"success_count": 0, "fail_count": 0}, ""
        assert _one_detail.running_status == TaskResult.Nothing.value, ""
        assert len(_one_detail.result_detail) == 0, "It should NOT have any running result because it is backup role."

    def __get_metadata_opts(self, review_data: Union[str, bytes, GroupState, NodeState, Task, Heartbeat],
                            zk_path: str, metadata_type: Type[Union[GroupState, NodeState, Task, Heartbeat]],
                            data_by_json_obj: Type[Union[GroupStateByJsonData, NodeStateByJsonData, TaskDataFromJsonData, HeartbeatFromJsonData]],
                            data_by_object: Type[Union[GroupStateByObject, NodeStateByObject, TaskDataFromObject, HeartbeatFromObject]]
                            ) -> Union[GroupStateData, NodeStateData, TaskData, HeartbeatData]:
        if review_data is None:
            _data, _state = self._client.get(path=zk_path)
            assert len(_data) != 0, "The data content of meta data *GroupState* should NOT be empty."
            print(f"[DEBUG in testing] path: {zk_path}, _data: {_data}")
            _meta_data_opt = data_by_json_obj(data=_data)
        else:
            if type(review_data) is metadata_type:
                _meta_data_opt = data_by_object(data=review_data)
            elif type(review_data) in [str, bytes]:
                _meta_data_opt = data_by_json_obj(data=review_data)
            else:
                raise TypeError(f"Doesn't support data type {type(review_data)} processing.")
        return _meta_data_opt
