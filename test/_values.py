"""
Here are some global variables for testing.
"""

from datetime import datetime
from typing import List

from smoothcrawler_cluster.model import (
    CrawlerStateRole,
    GroupState,
    Heartbeat,
    HeartState,
    NodeState,
    RunningContent,
    Task,
    TaskResult,
)

from ._config import Test_Task_API_Path, Test_Task_HTTP_Host

# # # # For Zookeeper
# # Zookeeper paths
Test_Zookeeper_Parent_Path = "/test"
Test_Zookeeper_Path = "/test/zk_path"
Test_Zookeeper_Not_Exist_Path = "/test/zk_not_exist_path"
# # Zookeeper values
Test_Zookeeper_String_Value = "This is test value in zookeeper"
Test_Zookeeper_Bytes_Value = b"This is test value in zookeeper"

# # # # For feature related crawler
# # Related state settings
_Crawler_Group_Name_Value: str = "sc-crawler-cluster"
_Crawler_Name_Value: str = "sc-crawler_1"
_Crawler_Role_Value: str = CrawlerStateRole.INITIAL.value
_Runner_Crawler_Value: int = 2
_Backup_Crawler_Value: int = 1
_Fail_Runner_Crawler_Value: int = 1
_Total_Crawler_Value: int = _Runner_Crawler_Value + _Backup_Crawler_Value
_State_Standby_ID_Value: str = "3"
_Empty_List_Value: list = []
_Empty_Dict_Value: dict = {}

# # Related task settings
_Task_Test_URL: str = f"http://{Test_Task_HTTP_Host}{Test_Task_API_Path}"
_Task_Real_URL: str = "https://www.example.com"
_One_Running_Content: dict = {
    "task_id": 0,
    "url": _Task_Test_URL,
    "method": "GET",
    "parameters": _Empty_Dict_Value,
    "header": _Empty_Dict_Value,
    "body": _Empty_Dict_Value,
}
_One_Running_Content_As_Object = RunningContent(
    task_id=0,
    url=_Task_Real_URL,
    method="GET",
    parameters=_Empty_Dict_Value,
    header=_Empty_Dict_Value,
    body=_Empty_Dict_Value,
)
_Task_Running_Content_Value: List[dict] = [_One_Running_Content]
_Task_Cookie_Value: dict = {}
_Task_Authorization_Value: dict = {}
_Task_In_Progressing_Id_Value: str = "0"
_Task_Running_Result: dict = {"success_count": 0, "fail_count": 0}
_Task_Running_State: str = str(TaskResult.NOTHING.value)
_Task_Result_Detail_Value: List[dict] = [
    {"task_id": 0, "state": "done", "status_code": 200, "response": "", "error_msg": None}
]

# # Related heartbeat settings
_Time_Value: datetime = datetime.now()
_Time_Format_Value: str = "%Y-%m-%d %H:%M:%S"
_Update_Value: str = "2s"
_Update_Timeout_Value: str = "4s"
_Heartbeat_Timeout_Value: str = "2"
_Heartbeat_State_Value: str = HeartState.HEALTHY.value
_Task_State_Value: str = TaskResult.PROCESSING.value

_Waiting_Time: int = 5


def generate_crawler_list(index: int) -> list:
    return [f"spider_{i}" for i in range(1, index + 1)]


# # # # For data objects: *State*, *Task*, *Heartbeat*
# # *GroupState*
_Test_Group_State_Data = {
    "total_crawler": _Total_Crawler_Value,
    "total_runner": _Runner_Crawler_Value,
    "total_backup": _Backup_Crawler_Value,
    "standby_id": _State_Standby_ID_Value,
    "current_crawler": generate_crawler_list(3),
    "current_runner": generate_crawler_list(2),
    "current_backup": generate_crawler_list(1),
    "fail_crawler": _Empty_List_Value,
    "fail_runner": _Empty_List_Value,
    "fail_backup": _Empty_List_Value,
}

# # *NodeState*
_Test_Node_State_Data = {"group": _Crawler_Group_Name_Value, "role": _Crawler_Role_Value}

# # *Task*
_Test_Task_Data = {
    "running_content": _Task_Running_Content_Value,
    "cookie": _Task_Cookie_Value,
    "authorization": _Task_Authorization_Value,
    "in_progressing_id": _Task_In_Progressing_Id_Value,
    "running_result": _Task_Running_Result,
    "running_status": _Task_Running_State,
    "result_detail": _Task_Result_Detail_Value,
}

# # *Heartbeat*
_Test_Heartbeat_Data = {
    "heart_rhythm_time": _Time_Value.strftime(_Time_Format_Value),
    "time_format": _Time_Format_Value,
    "update_time": _Update_Value,
    "update_timeout": _Update_Timeout_Value,
    "heart_rhythm_timeout": _Heartbeat_Timeout_Value,
    "healthy_state": _Heartbeat_State_Value,
    "task_state": _Task_State_Value,
}


def setup_group_state(reset: bool = False) -> GroupState:
    state = GroupState()
    # state.role = _Test_Group_State_Data["role"]
    state.total_crawler = _Test_Group_State_Data["total_crawler"]
    state.total_runner = _Test_Group_State_Data["total_runner"]
    state.total_backup = _Test_Group_State_Data["total_backup"]
    if reset is True:
        state.current_crawler = _Empty_List_Value
        state.current_runner = _Empty_List_Value
        state.current_backup = _Empty_List_Value
    else:
        state.current_crawler = _Test_Group_State_Data["current_crawler"]
        state.current_runner = _Test_Group_State_Data["current_runner"]
        state.current_backup = _Test_Group_State_Data["current_backup"]
    state.fail_crawler = _Test_Group_State_Data["fail_crawler"]
    state.fail_runner = _Test_Group_State_Data["fail_runner"]
    state.fail_backup = _Test_Group_State_Data["fail_backup"]
    state.standby_id = _Test_Group_State_Data["standby_id"]
    return state


def setup_node_state() -> NodeState:
    state = NodeState()
    state.group = _Test_Node_State_Data["group"]
    state.role = _Test_Node_State_Data["role"]
    return state


def setup_task(reset: bool = False) -> Task:
    task = Task()
    if reset is True:
        task.running_content = _Empty_List_Value
    else:
        task.running_content = _Test_Task_Data["running_content"]
    task.cookie = _Test_Task_Data["cookie"]
    task.authorization = _Test_Task_Data["authorization"]
    task.in_progressing_id = _Test_Task_Data["in_progressing_id"]
    task.running_result = _Test_Task_Data["running_result"]
    task.running_status = _Test_Task_Data["running_status"]
    if reset is True:
        task.result_detail = _Empty_List_Value
    else:
        task.result_detail = _Test_Task_Data["result_detail"]
    print(f"[DEBUG - setup_task] _task: {task.to_readable_object()}")
    return task


def setup_heartbeat() -> Heartbeat:
    heartbeat = Heartbeat()
    heartbeat.heart_rhythm_time = _Test_Heartbeat_Data["heart_rhythm_time"]
    heartbeat.time_format = _Test_Heartbeat_Data["time_format"]
    heartbeat.update_time = _Test_Heartbeat_Data["update_time"]
    heartbeat.update_timeout = _Test_Heartbeat_Data["update_timeout"]
    heartbeat.heart_rhythm_timeout = _Test_Heartbeat_Data["heart_rhythm_timeout"]
    heartbeat.healthy_state = _Test_Heartbeat_Data["healthy_state"]
    heartbeat.task_state = _Test_Heartbeat_Data["task_state"]
    return heartbeat
