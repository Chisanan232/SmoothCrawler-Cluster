from datetime import datetime
from kazoo.client import KazooClient
from smoothcrawler_cluster.model import TaskResult, HeartState, GroupState, NodeState, Task, Heartbeat
from multiprocessing.managers import DictProxy
from typing import Dict, Type, Union
import traceback
import re

from ._metadata_values import (
    # For GroupState metadata
    GroupStateData, GroupStateByObject, GroupStateByJsonData,
    # For NodeState metadata
    NodeStateData, NodeStateByObject, NodeStateByJsonData,
    # For Task metadata
    TaskData, TaskDataFromObject, TaskDataFromJsonData,
    # For Heartbeat metadata
    HeartbeatData, HeartbeatFromObject, HeartbeatFromJsonData
)
from ._values import _Task_Running_Content_Value, _Time_Format_Value
from .integration_test._test_utils._instance_value import _TestValue, _ZKNodePathUtils


_Testing_Value: _TestValue = _TestValue()


def _equal_assertion(under_test, expect=None, none_check: bool = False) -> None:
    if none_check is True:
        assert under_test is not None,\
            f"The value should be the same. Under test: {under_test}, expected value: not None."
    else:
        assert under_test == expect,\
            f"The value should be the same. Under test: {under_test}, expected value: {expect}."


class Verify:

    @classmethod
    def exception(cls, exception: Union[Exception, Dict[str, Exception]]) -> None:

        def _print_traced_exception(e_name: str, e: Exception) -> str:
            print(f"[DEBUG in Verify] _e_name: {e_name}")
            print(f"[DEBUG in Verify] _e: {e}")
            return f"=========================== {e_name} ===========================" \
                   f"{traceback.format_exception(e)}" \
                   f"================================================================="

        print(f"[DEBUG in Verify] exception: {exception}")
        if type(exception) in [dict, DictProxy]:
            has_except = list(filter(lambda e: e is not None, exception.values()))
            if len(has_except) == 0:
                assert True, "It doesn't have any exception in any thread or process."
            elif len(has_except) == 1:
                raise has_except[0]
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

    def group_state_is_not_empty(
            self,
            runner: int,
            backup: int,
            standby_id: str,
            review_data: Union[str, bytes, GroupState] = None,
    ) -> None:
        group_state = self._generate_group_state_data_opt(review_data)

        _equal_assertion(group_state.total_crawler, runner + backup)
        _equal_assertion(group_state.total_runner, runner)
        _equal_assertion(group_state.total_backup, backup)
        _equal_assertion(group_state.standby_id, standby_id)

    def group_state_info(
            self,
            runner: int,
            backup: int,
            fail_runner: int = 0,
            fail_runner_name: str = None,
            standby_id: str = None,
            index_sep_char: str = "_",
            review_data: Union[str, bytes, GroupState] = None,
    ) -> None:
        group_state = self._generate_group_state_data_opt(review_data)

        # Verify total parts
        assert group_state.total_crawler == runner + backup, \
            f"The attribute 'total_crawler' should be '{runner + backup}'."
        assert group_state.total_runner == runner, f"The attribute 'total_runner' should be '{runner}'."
        assert group_state.total_backup == backup - fail_runner,\
            f"The attribute 'total_backup' should be '{backup - fail_runner}'."

        # # Verify current section
        self.group_state_current_section(review_data=review_data,
                                         runner=runner,
                                         backup=backup,
                                         fail_runner=fail_runner,
                                         index_sep_char=index_sep_char)

        # Verify standby_id
        if standby_id is not None:
            assert group_state.standby_id == standby_id, f"The attribute 'standby_id' should be '{standby_id}'."
        standby_id_checksum_list = list(
            map(lambda _crawler: group_state.standby_id in _crawler, group_state.current_backup))
        if len(group_state.current_backup) != 0:
            assert True in standby_id_checksum_list, \
                "The standby ID (the index of crawler name) should be included in the one name of backup crawlers list."

        # Verify fail parts
        assert len(group_state.fail_crawler) == fail_runner,\
            f"The length of attribute 'fail_crawler' should be '{fail_runner}'."
        if fail_runner != 0 and fail_runner_name is not None:
            assert False not in [fail_runner_name in crawler for crawler in group_state.fail_crawler], ""
        assert len(group_state.fail_runner) == fail_runner,\
            f"The length of attribute 'fail_runner' should be '{fail_runner}'."
        if fail_runner != 0 and fail_runner_name is not None:
            assert False not in [fail_runner_name in crawler for crawler in group_state.fail_runner], ""
        assert len(group_state.fail_backup) == 0, f"The length of attribute 'fail_backup' should be '{0}'."

    def group_state_current_section(
            self,
            runner: int,
            backup: int,
            fail_runner: int = 0,
            fail_runner_name: str = None,
            index_sep_char: str = "_",
            review_data: Union[str, bytes, GroupState, GroupStateData] = None,
            verify_crawler: bool = True,
            verify_runner: bool = True,
            verify_backup: bool = True,
    ) -> None:
        if isinstance(review_data, GroupStateData):
            group_state = review_data
        else:
            group_state = self._generate_group_state_data_opt(review_data)

        # Verify current_crawler
        if verify_crawler is True:
            assert len(group_state.current_crawler) == runner + backup - fail_runner,\
                f"The length of attribute 'current_crawler' should be '{runner + backup - fail_runner}'."
            assert len(group_state.current_crawler) == len(set(group_state.current_crawler)),\
                "Attribute *current_crawler* should NOT have any element is repeated."
            # TODO: How to let fail crawler name to be parameter?
            if fail_runner != 0 and fail_runner_name is not None:
                assert False not in [fail_runner_name not in crawler for crawler in group_state.current_crawler],\
                    ""

        # Verify current_runner
        if verify_runner is True:
            assert len(group_state.current_runner) == runner,\
                f"The length of attribute 'current_runner' should be '{runner}'."
            if fail_runner == 0:
                runer_checksum_iter = map(
                    lambda _crawler: int(_crawler.split(index_sep_char)[-1]) <= runner,
                    group_state.current_runner)
                assert False not in list(runer_checksum_iter),\
                    f"The index of all crawler name should be <= {runner} (the count of runner)."
            if fail_runner != 0 and fail_runner_name is not None:
                assert False not in [fail_runner_name not in crawler for crawler in group_state.current_runner],\
                    ""

        # Verify current_backup
        if verify_backup is True:
            assert len(group_state.current_backup) == backup - fail_runner,\
                f"The length of attribute 'current_backup' should be '{backup - fail_runner}'."
            if fail_runner == 0:
                backup_checksum_iter = map(lambda _crawler: int(_crawler.split(index_sep_char)[-1]) > backup,
                                           group_state.current_backup)
                assert False not in list(backup_checksum_iter),\
                    f"The index of all crawler name should be > {backup} (the count of runner)."

    def _generate_group_state_data_opt(self, review_data: Union[str, bytes, GroupState] = None) -> GroupStateData:
        return self.__get_metadata_opts(review_data,
                                        metadata_type=GroupState,
                                        zk_path=_Testing_Value.group_state_zookeeper_path,
                                        data_by_object=GroupStateByObject,
                                        data_by_json_obj=GroupStateByJsonData)

    def node_state_is_not_empty(self, role: str, group: str, review_data: Union[str, bytes, NodeState] = None) -> None:
        node_state = self._generate_node_state_data_opt(review_data)

        _equal_assertion(node_state.role, role)
        _equal_assertion(node_state.group, group)

    def all_node_state_role(
            self,
            runner: int,
            backup: int,
            expected_role: dict,
            expected_group: dict,
            start_index: int = 1,
    ) -> None:
        state_paths = _ZKNodePathUtils.all_node_state(size=runner + backup, start_index=start_index)
        for path in list(state_paths):
            data, state = self._client.get(path=path)
            chksum = re.search(r"[0-9]{1,3}", path)
            if chksum is not None:
                self.one_node_state(data,
                                    role=expected_role[chksum.group(0)].value,
                                    group=expected_group[chksum.group(0)])
            else:
                assert False, ""

    def one_node_state(self, node_state: Union[str, bytes, NodeStateData], role: str = None, group: str = None) -> None:
        node_state = self._generate_node_state_data_opt(node_state)

        if role is not None:
            assert node_state.role == role, f"The attribute 'role' should be '{role}'."
        if group is not None:
            assert node_state.group == group, f"The attribute 'group' should be '{group}'."

    def _generate_node_state_data_opt(self, node_state: Union[str, bytes, NodeStateData] = None) -> NodeStateData:
        return self.__get_metadata_opts(node_state,
                                        metadata_type=NodeState,
                                        zk_path=_Testing_Value.node_state_zookeeper_path,
                                        data_by_object=NodeStateByObject,
                                        data_by_json_obj=NodeStateByJsonData)

    def task_is_not_empty(
            self,
            running_content: list = [],
            cookies: dict = {},
            authorization: dict = {},
            running_result: dict = None,
            running_status: str = None,
            result_detail: list = [],
            review_data: Union[str, bytes, Task] = None,
    ) -> None:
        task = self._generate_task_data_opt(review_data)

        _equal_assertion(task.running_content, running_content)
        _equal_assertion(task.cookies, cookies)
        _equal_assertion(task.authorization, authorization)
        if running_status is None:
            running_status = TaskResult.NOTHING.value
        _equal_assertion(task.running_status, running_status)
        if running_result is None:
            running_result = {"success_count": 0, "fail_count": 0}
        _equal_assertion(task.running_result, running_result)
        _equal_assertion(task.result_detail, result_detail)

    def all_task_info(
            self,
            runner: int,
            backup: int,
            running_content_len: int = None,
            cookies: dict = None,
            authorization: dict = None,
            in_progressing_id: str = None,
            running_status: str = None,
            running_result: dict = None,
            result_detail_len: int = None,
            start_index: int = 1,
    ) -> None:
        task_paths = _ZKNodePathUtils.all_task(size=runner + backup, start_index=start_index)
        for path in list(task_paths):
            data, state = self._client.get(path=path)
            print(f"[DEBUG in testing] _task_path: {path}, _task: {data}")
            self.one_task_info(data,
                               running_content_len=running_content_len,
                               cookies=cookies,
                               authorization=authorization,
                               in_progressing_id=in_progressing_id,
                               running_status=running_status,
                               running_result=running_result,
                               result_detail_len=result_detail_len)

    def one_task_info(
            self,
            task: Union[str, bytes, Task],
            running_content_len: int = None,
            cookies: dict = None,
            authorization: dict = None,
            in_progressing_id: str = None,
            running_status: str = None,
            running_result: dict = None,
            result_detail_len: int = None,
    ) -> None:
        task = self._generate_task_data_opt(task)

        assert isinstance(task.running_content, list), "The data type of attribute 'running_content' should be list."
        if running_content_len is not None:
            assert len(task.running_content) == running_content_len,\
                f"The length of attribute 'running_content' should be {len(task.running_content)}."
        if cookies is not None:
            assert task.cookies == cookies, f"The attribute 'cookie' should be '{cookies}'."
        if authorization is not None:
            assert task.authorization == authorization, f"The attribute 'authorization' should be '{authorization}'."
        if in_progressing_id is not None:
            assert task.in_progressing_id == in_progressing_id,\
                f"The attribute 'in_progressing_id' should be '{in_progressing_id}'."
        if running_status is not None:
            assert task.running_status == running_status,\
                f"The attribute 'running_status' should be '{running_status}'."
        if running_result is not None:
            assert ("success_count", "fail_count") == tuple(task.running_result.keys()), \
                "The keys of attribute 'running_result' should be 'success_count' and 'fail_count'."
            assert task.running_result == running_result,\
                f"The attribute 'running_result' should be '{running_result}'."
        if result_detail_len is not None:
            assert len(task.result_detail) == result_detail_len, f"The authorization should be '{authorization}'."

    def all_task_detail(self, runner: int, backup: int, expected_task_result: dict, start_index: int = 1) -> None:
        task_paths = _ZKNodePathUtils.all_task(size=runner + backup, start_index=start_index)
        for path in list(task_paths):
            data, state = self._client.get(path=path)
            print(f"[DEBUG in testing] _task_path: {path}, _task: {data}")
            self.one_task_result_detail(data, task_path=path, expected_task_result=expected_task_result)

    def one_task_result_detail(self, task: Union[str, bytes, Task], task_path: str, expected_task_result: dict) -> None:
        task = self._generate_task_data_opt(task)

        assert task is not None, ""
        chksum = re.search(r"[0-9]{1,3}", task_path)
        if chksum is not None:
            chk_detail_assert = getattr(self, f"_chk_{expected_task_result[chksum.group(0)]}_task_detail")
            chk_detail_assert(task)
        else:
            assert False, ""

    def _generate_task_data_opt(self, task: Union[str, bytes, Task] = None) -> TaskData:
        return self.__get_metadata_opts(task,
                                        metadata_type=Task,
                                        zk_path=_Testing_Value.task_zookeeper_path,
                                        data_by_object=TaskDataFromObject,
                                        data_by_json_obj=TaskDataFromJsonData)

    def heartbeat_is_not_empty(self, review_data: Union[str, bytes, Heartbeat] = None) -> None:
        heartbeat = self._generate_heartbeat_data_opt(review_data)

        _equal_assertion(heartbeat.heart_rhythm_time, none_check=True)
        _equal_assertion(heartbeat.update_time, none_check=True)
        _equal_assertion(heartbeat.time_format, none_check=True)
        _equal_assertion(heartbeat.update_timeout, none_check=True)
        _equal_assertion(heartbeat.heart_rhythm_timeout, none_check=True)
        _equal_assertion(heartbeat.task_state, none_check=True)
        _equal_assertion(heartbeat.healthy_state, none_check=True)

    def all_heartbeat_info(self, runner: int, backup: int, start_index: int = 1) -> None:
        heartbeat_paths = _ZKNodePathUtils.all_heartbeat(size=runner + backup, start_index=start_index)
        for path in heartbeat_paths:
            heartbeat, state = self._client.get(path=path)
            print(f"[DEBUG] _path: {path}, _task: {heartbeat}")
            self.one_heartbeat(heartbeat)

    def one_heartbeat(self, heartbeat: Union[str, bytes, Heartbeat]) -> None:
        heartbeat = self._generate_heartbeat_data_opt(heartbeat)

        assert heartbeat.heart_rhythm_time is not None and heartbeat.heart_rhythm_time != "", ""
        assert heartbeat.time_format == _Time_Format_Value, ""
        assert re.search(r"[0-9]{1,2}[smh]", heartbeat.update_time) is not None, ""
        assert re.search(r"[0-9]{1,2}[smh]", heartbeat.update_timeout) is not None, ""
        assert re.search(r"[0-9]{1,2}", heartbeat.heart_rhythm_timeout) is not None, ""
        heart_rhythm_datetime = datetime.strptime(heartbeat.heart_rhythm_time, heartbeat.time_format)
        now_datetime = datetime.now()
        diff_datetime = now_datetime - heart_rhythm_datetime
        assert diff_datetime.total_seconds() < \
               float(heartbeat.update_time[:-1]) + float(heartbeat.update_timeout[:-1]), ""
        assert heartbeat.healthy_state == HeartState.HEALTHY.value, ""
        assert heartbeat.task_state == TaskResult.NOTHING.value, ""

    def _generate_heartbeat_data_opt(self, heartbeat: Union[str, bytes, Heartbeat] = None) -> HeartbeatData:
        return self.__get_metadata_opts(heartbeat,
                                        metadata_type=Heartbeat,
                                        zk_path=_Testing_Value.heartbeat_zookeeper_path,
                                        data_by_object=HeartbeatFromObject,
                                        data_by_json_obj=HeartbeatFromJsonData)

    @classmethod
    def _check_running_status(cls, running_flag: Dict[str, bool]) -> None:
        if False not in running_flag.values():
            assert True, "Work finely."
        elif False in running_flag:
            assert False, "It should NOT have any thread gets any exception in running."

    @classmethod
    def _chk_available_task_detail(cls, one_detail: TaskData) -> None:
        assert len(one_detail.running_content) == 0, ""
        assert one_detail.in_progressing_id == "-1", ""
        assert one_detail.running_result == {"success_count": 1, "fail_count": 0}, ""
        assert one_detail.running_status == TaskResult.DONE.value, ""
        assert one_detail.result_detail[0] == {
            "task_id": _Task_Running_Content_Value[0]["task_id"],
            "state": TaskResult.DONE.value,
            "status_code": 200,
            "response": "Example Domain",
            "error_msg": None
        }, "The detail should be completely same as above."

    @classmethod
    def _chk_nothing_task_detail(cls, one_detail: TaskData) -> None:
        assert len(one_detail.running_content) == 0, ""
        assert one_detail.in_progressing_id == "0", ""
        assert one_detail.running_result == {"success_count": 0, "fail_count": 0}, ""
        assert one_detail.running_status == TaskResult.NOTHING.value, ""
        assert len(one_detail.result_detail) == 0, "It should NOT have any running result because it is backup role."

    @classmethod
    def _chk_processing_task_detail(cls, one_detail: TaskData) -> None:
        assert len(one_detail.running_content) == 1, ""
        assert one_detail.in_progressing_id == "0", ""
        assert one_detail.running_result == {"success_count": 0, "fail_count": 0}, ""
        assert one_detail.running_status == TaskResult.PROCESSING.value, ""
        assert len(one_detail.result_detail) == 0, "It should NOT have any running result because it is backup role."

    @classmethod
    def _chk_backup_task_detail(cls, one_detail: TaskData) -> None:
        assert len(one_detail.running_content) == 0, ""
        assert one_detail.in_progressing_id == "-1", ""
        assert one_detail.running_result == {"success_count": 0, "fail_count": 0}, ""
        assert one_detail.running_status == TaskResult.NOTHING.value, ""
        assert len(one_detail.result_detail) == 0, "It should NOT have any running result because it is backup role."

    def __get_metadata_opts(
            self,
            review_data: Union[str, bytes, GroupState, NodeState, Task, Heartbeat],
            zk_path: str,
            metadata_type: Type[Union[GroupState, NodeState, Task, Heartbeat]],
            data_by_json_obj:
            Type[Union[GroupStateByJsonData, NodeStateByJsonData, TaskDataFromJsonData, HeartbeatFromJsonData]],
            data_by_object: Type[Union[GroupStateByObject, NodeStateByObject, TaskDataFromObject, HeartbeatFromObject]],
    ) -> Union[GroupStateData, NodeStateData, TaskData, HeartbeatData]:
        if review_data is None:
            data, state = self._client.get(path=zk_path)
            assert len(data) != 0, "The data content of meta data *GroupState* should NOT be empty."
            print(f"[DEBUG in testing] path: {zk_path}, _data: {data}")
            meta_data_opt = data_by_json_obj(data=data)
        else:
            if isinstance(review_data, metadata_type):
                meta_data_opt = data_by_object(data=review_data)
            elif isinstance(review_data, (str, bytes)):
                meta_data_opt = data_by_json_obj(data=review_data)
            else:
                raise TypeError(f"Doesn't support data type {type(review_data)} processing.")
        return meta_data_opt
