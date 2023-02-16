import multiprocessing as mp
from unittest.mock import MagicMock, patch

import pytest
from kazoo.client import KazooClient

from smoothcrawler_cluster.crawler.crawlers import ZookeeperCrawler
from smoothcrawler_cluster.model import CrawlerStateRole, GroupState, NodeState
from smoothcrawler_cluster.register import Register

from .._config import Zookeeper_Hosts
from .._values import (
    _Backup_Crawler_Value,
    _Crawler_Group_Name_Value,
    _Runner_Crawler_Value,
)
from .._verify import VerifyMetaData
from ._test_utils._instance_value import _TestValue, _ZKNodePathUtils
from ._test_utils._zk_testsuite import ZK, ZKNode, ZKTestSpec
from .crawler._spec import generate_crawler_name, generate_metadata_opts

_Manager = mp.Manager()
_Testing_Value: _TestValue = _TestValue()


class TestZookeeperCrawlerSingleInstance(ZKTestSpec):

    _verify_metadata = VerifyMetaData()

    @pytest.fixture(scope="function")
    def uit_object(self) -> Register:
        self._pytest_zk_client = KazooClient(hosts=Zookeeper_Hosts)
        self._pytest_zk_client.start()

        self._verify_metadata.initial_zk_session(client=self._pytest_zk_client)

        zk_crawler = ZookeeperCrawler(
            runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, initial=False, zk_hosts=Zookeeper_Hosts
        )
        return Register(
            name=generate_crawler_name(zk_crawler),
            path=zk_crawler._zk_path,
            metadata_opts_callback=generate_metadata_opts(zk_crawler),
            lock=zk_crawler.distributed_lock_adapter,
        )

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE])
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE])
    def test_register_group_state_with_not_exist_node(self, uit_object: Register):
        self.__opt_register_group_state_and_verify_result(uit_object, node_exist=False, ensure=False)

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.GROUP_STATE: _Testing_Value.group_state_data_str})
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE])
    def test_register_group_state_with_existed_node(self, uit_object: Register):
        self.__opt_register_group_state_and_verify_result(uit_object, node_exist=True, ensure=False)

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE])
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE])
    def test_ensure_register_group_state_with_not_exist_node(self, uit_object: Register):
        self.__opt_register_group_state_and_verify_result(uit_object, node_exist=False, ensure=True)

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.GROUP_STATE: _Testing_Value.group_state_data_str})
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE])
    def test_ensure_register_group_state_with_existed_node(self, uit_object: Register):
        self.__opt_register_group_state_and_verify_result(uit_object, node_exist=True, ensure=True)

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE])
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE])
    def test_ensure_register_group_state_timeout(self, uit_object: Register):
        try:
            # Run the target function to test
            uit_object.group_state(
                runner=_Runner_Crawler_Value,
                backup=_Backup_Crawler_Value,
                ensure=True,
                ensure_wait=0.5,
                ensure_timeout=3,
            )
        except TimeoutError as e:
            expected_err_msg = "It gets timeout of registering meta data *GroupState* to Zookeeper cluster."
            assert str(e) == expected_err_msg, f"The expected error message should be {expected_err_msg}."
        else:
            assert False, ""

    def __opt_register_group_state_and_verify_result(self, register: Register, node_exist: bool, ensure: bool):
        exist_node = self._exist_node(path=_Testing_Value.group_state_zookeeper_path)
        if node_exist is True:
            assert exist_node is not None, ""
        else:
            assert exist_node is None, ""

        if ensure:
            ut_runner = 1
            ut_backup = 0
        else:
            ut_runner = _Runner_Crawler_Value
            ut_backup = _Backup_Crawler_Value

        # Run the target function to test
        register.group_state(
            runner=ut_runner,
            backup=ut_backup,
            ensure=ensure,
            ensure_wait=0.5,
            ensure_timeout=3,
        )

        exist_node = self._exist_node(path=_Testing_Value.group_state_zookeeper_path)
        assert exist_node is not None, ""
        self._verify_metadata.group_state_is_not_empty(runner=ut_runner, backup=ut_backup, standby_id="0")

    @ZK.reset_testing_env(path=[ZKNode.NODE_STATE])
    @ZK.remove_node_finally(path=[ZKNode.NODE_STATE])
    def test_register_node_state_with_not_exist_node(self, uit_object: Register):
        self.__opt_register_node_state_and_verify_result(uit_object, node_exist=False)

    @ZK.reset_testing_env(path=[ZKNode.NODE_STATE])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.NODE_STATE: _Testing_Value.node_state_data_str})
    @ZK.remove_node_finally(path=[ZKNode.NODE_STATE])
    def test_register_node_state_with_existed_node(self, uit_object: Register):
        self.__opt_register_node_state_and_verify_result(uit_object, node_exist=True)

    def __opt_register_node_state_and_verify_result(self, register: Register, node_exist: bool):
        exist_node = self._exist_node(path=_Testing_Value.node_state_zookeeper_path)
        if node_exist is True:
            assert exist_node is not None, ""
        else:
            assert exist_node is None, ""

        # Run the target function to test
        register.node_state()

        exist_node = self._exist_node(path=_Testing_Value.node_state_zookeeper_path)
        assert exist_node is not None, ""
        self._verify_metadata.node_state_is_not_empty(
            role=CrawlerStateRole.INITIAL.value, group=_Crawler_Group_Name_Value
        )

    @ZK.reset_testing_env(path=[ZKNode.TASK])
    @ZK.remove_node_finally(path=[ZKNode.TASK])
    def test_register_task_with_not_exist_node(self, uit_object: Register):
        self.__opt_register_task_and_verify_result(uit_object, node_exist=False)

    @ZK.reset_testing_env(path=[ZKNode.TASK])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.TASK: _Testing_Value.task_data_str})
    @ZK.remove_node_finally(path=[ZKNode.TASK])
    def test_register_task_with_existed_node(self, uit_object: Register):
        self.__opt_register_task_and_verify_result(uit_object, node_exist=True)

    def __opt_register_task_and_verify_result(self, register: Register, node_exist: bool):
        exist_node = self._exist_node(path=_Testing_Value.task_zookeeper_path)
        if node_exist is True:
            assert exist_node is not None, ""
        else:
            assert exist_node is None, ""

        # Run the target function to test
        register.task()

        exist_node = self._exist_node(path=_Testing_Value.task_zookeeper_path)
        assert exist_node is not None, ""
        self._verify_metadata.task_is_not_empty()

    @ZK.reset_testing_env(path=[ZKNode.HEARTBEAT])
    @ZK.remove_node_finally(path=[ZKNode.HEARTBEAT])
    def test_register_heartbeat_with_not_exist_node(self, uit_object: Register):
        self.__opt_register_heartbeat_and_verify_result(uit_object, node_exist=False)

    @ZK.reset_testing_env(path=[ZKNode.HEARTBEAT])
    @ZK.add_node_with_value_first(path_and_value={ZKNode.HEARTBEAT: _Testing_Value.heartbeat_data_str})
    @ZK.remove_node_finally(path=[ZKNode.HEARTBEAT])
    def test_register_heartbeat_with_existed_node(self, uit_object: Register):
        self.__opt_register_heartbeat_and_verify_result(uit_object, node_exist=True)

    def __opt_register_heartbeat_and_verify_result(self, register: Register, node_exist: bool):
        exist_node = self._exist_node(path=_Testing_Value.heartbeat_zookeeper_path)
        if node_exist is True:
            assert exist_node is not None, ""
        else:
            assert exist_node is None, ""

        # Run the target function to test
        register.heartbeat()

        exist_node = self._exist_node(path=_Testing_Value.heartbeat_zookeeper_path)
        assert exist_node is not None, ""
        self._verify_metadata.heartbeat_is_not_empty()

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    def test_register_metadata_with_not_exist_node(self, uit_object: Register):
        self.__operate_register_metadata_and_verify_result(register=uit_object, exist_node=False)

    @ZK.reset_testing_env(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    @ZK.add_node_with_value_first(
        path_and_value={
            ZKNode.GROUP_STATE: _Testing_Value.group_state_data_str,
            ZKNode.NODE_STATE: _Testing_Value.node_state_data_str,
            ZKNode.TASK: _Testing_Value.task_data_str,
            ZKNode.HEARTBEAT: _Testing_Value.heartbeat_data_str,
        }
    )
    @ZK.remove_node_finally(path=[ZKNode.GROUP_STATE, ZKNode.NODE_STATE, ZKNode.TASK, ZKNode.HEARTBEAT])
    def test_register_metadata_with_exist_node(self, uit_object: Register):
        self.__operate_register_metadata_and_verify_result(register=uit_object, exist_node=True)

    def __operate_register_metadata_and_verify_result(self, register: Register, exist_node: bool):
        def _verify_exist(should_be_none: bool) -> None:
            exist_group_state_node = self._exist_node(path=_Testing_Value.group_state_zookeeper_path)
            exist_node_state_node = self._exist_node(path=_Testing_Value.node_state_zookeeper_path)
            exist_task_node = self._exist_node(path=_Testing_Value.task_zookeeper_path)
            exist_heartbeat_node = self._exist_node(path=_Testing_Value.heartbeat_zookeeper_path)
            if should_be_none is True:
                assert exist_group_state_node is None, ""
                assert exist_node_state_node is None, ""
                assert exist_task_node is None, ""
                assert exist_heartbeat_node is None, ""
            else:
                assert exist_group_state_node is not None, ""
                assert exist_node_state_node is not None, ""
                assert exist_task_node is not None, ""
                assert exist_heartbeat_node is not None, ""

        _verify_exist(should_be_none=(exist_node is False))

        # Operate target method to test
        register.metadata(
            runner=_Runner_Crawler_Value,
            backup=_Backup_Crawler_Value,
            ensure=False,
            ensure_wait=0.5,
            ensure_timeout=3,
        )

        _verify_exist(should_be_none=False)

        self._verify_metadata.group_state_is_not_empty(
            runner=_Runner_Crawler_Value, backup=_Backup_Crawler_Value, standby_id="0"
        )
        self._verify_metadata.node_state_is_not_empty(
            role=CrawlerStateRole.INITIAL.value, group=_Crawler_Group_Name_Value
        )
        self._verify_metadata.task_is_not_empty()
        self._verify_metadata.heartbeat_is_not_empty()
