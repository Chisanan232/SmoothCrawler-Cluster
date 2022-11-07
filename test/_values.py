"""
Here are some global variables for testing.
"""

from smoothcrawler_cluster.model import CrawlerStateRole, TaskResult, HeartState, State, Task, Heartbeat
from datetime import datetime


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
_Crawler_Name_Value: str = "sc-crawler_1"
_Crawler_Role_Value: str = CrawlerStateRole.Initial.value
_Runner_Crawler_Value: int = 2
_Backup_Crawler_Value: int = 1
_Total_Crawler_Value: int = _Runner_Crawler_Value + _Backup_Crawler_Value
_State_Standby_ID_Value: str = "3"
_Empty_List_Value: list = []

# # Related task settings
_Task_Result_Value: str = TaskResult.Nothing.value
_Task_Content_Value: dict = {}

# # Related heartbeat settings
_Time_Value: datetime = datetime.now()
_Time_Format_Value: str = "%Y-%m-%d %H:%M:%S"
_Update_Value: str = "2s"
_Update_Timeout_Value: str = "4s"
_Heartbeat_Timeout_Value: str = "2"
_Heartbeat_State_Value: str = HeartState.Healthy.value
_Task_State_Value: str = TaskResult.Processing.value

_Waiting_Time: int = 5


def generate_crawler_list(index: int) -> list:
    return [f"spider_{i}" for i in range(1, index + 1)]


# # # # For data objects: *State*, *Task*, *Heartbeat*
# # *State*
_Test_State_Data = {
    "role": _Crawler_Role_Value,
    "total_crawler": _Total_Crawler_Value,
    "total_runner": _Runner_Crawler_Value,
    "total_backup": _Backup_Crawler_Value,
    "standby_id": _State_Standby_ID_Value,
    "current_crawler": generate_crawler_list(3),
    "current_runner": generate_crawler_list(2),
    "current_backup": generate_crawler_list(1),
    "fail_crawler": _Empty_List_Value,
    "fail_runner": _Empty_List_Value,
    "fail_backup": _Empty_List_Value
}

# # *Task*
_Test_Task_Data = {
    "task_content": _Task_Content_Value,
    "task_result": _Task_Result_Value
}

# # *Heartbeat*
_Test_Heartbeat_Data = {
    "heart_rhythm_time": _Time_Value.strftime(_Time_Format_Value),
    "time_format": _Time_Format_Value,
    "update_time": _Update_Value,
    "update_timeout": _Update_Timeout_Value,
    "heart_rhythm_timeout": _Heartbeat_Timeout_Value,
    "healthy_state": _Heartbeat_State_Value,
    "task_state": _Task_State_Value
}


def setup_state(reset: bool = False) -> State:
    _state = State()
    _state.role = _Test_State_Data["role"]
    _state.total_crawler = _Test_State_Data["total_crawler"]
    _state.total_runner = _Test_State_Data["total_runner"]
    _state.total_backup = _Test_State_Data["total_backup"]
    if reset is True:
        _state.current_crawler = _Empty_List_Value
        _state.current_runner = _Empty_List_Value
        _state.current_backup = _Empty_List_Value
    else:
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
    _heartbeat.heart_rhythm_time = _Test_Heartbeat_Data["heart_rhythm_time"]
    _heartbeat.time_format = _Test_Heartbeat_Data["time_format"]
    _heartbeat.update_time = _Test_Heartbeat_Data["update_time"]
    _heartbeat.update_timeout = _Test_Heartbeat_Data["update_timeout"]
    _heartbeat.heart_rhythm_timeout = _Test_Heartbeat_Data["heart_rhythm_timeout"]
    _heartbeat.healthy_state = _Test_Heartbeat_Data["healthy_state"]
    _heartbeat.task_state = _Test_Heartbeat_Data["task_state"]
    return _heartbeat
