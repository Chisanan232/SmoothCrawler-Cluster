import re
import traceback
from unittest.mock import MagicMock, Mock

import pytest

from smoothcrawler_cluster._utils import MetaDataUtil, parse_timer
from smoothcrawler_cluster.model import GroupState, Heartbeat, NodeState, Task


class TestInitModule:
    @pytest.mark.parametrize("timer", ["2s", "2.5s", "2m", "2.5m", "2h", "2.5h"])
    def test_parse_timer_with_valid_value(self, timer: str):
        is_float: bool = False
        if "." in timer:
            is_float = True

        try:
            parsed_timer = parse_timer(timer=timer)
        except Exception:
            assert False, f"It should parse the timer value correctly. \n {traceback.format_exc()}"
        else:
            if is_float is True:
                assert isinstance(parsed_timer, float), "Its data type should be 'float'."
            else:
                assert isinstance(parsed_timer, int), "Its data type should be 'int'."
            timer_unit = timer[-1]
            if timer_unit == "s":
                original_timer_val = parsed_timer
            elif timer_unit == "m":
                original_timer_val = float(parsed_timer / 60)
            elif timer_unit == "h":
                original_timer_val = float(parsed_timer / 60 / 60)
            else:
                assert False, "It has issue in testing implement, please let developer to check it."
            if ".0" in str(original_timer_val):
                original_timer_val = int(original_timer_val)
            assert str(original_timer_val) in timer, f"The parsed value must be in the timer '{timer}'."

    @pytest.mark.parametrize("timer", ["2i", "2.6i", "test", "test_s"])
    def test_parse_timer_with_invalid_value(self, timer: str):
        try:
            parse_timer(timer=timer)
        except Exception as e:
            timer_chksum = re.search(r"[0-9]", timer)
            if timer_chksum is not None:
                assert isinstance(e, ValueError), "It should raise a 'ValueError' exception."
                assert "It only supports 's' (seconds), 'm' (minutes) or 'h' (hours) setting value." in str(
                    e
                ), "The error message is not correct."
            else:
                assert isinstance(e, ValueError), "It should raise a 'ValueError' exception."
                assert f"Invalid value {timer[:-1]}. It should be an integer format value." in str(
                    e
                ), "The error message is not correct."


class InvalidObject:
    pass


class TestInitModuleMetaDataUtil:
    @pytest.fixture(scope="function")
    def metadata_util(self) -> MetaDataUtil:
        return MetaDataUtil(converter=Mock(), client=Mock())

    @pytest.mark.parametrize("as_object", [GroupState, NodeState, Task, Heartbeat, InvalidObject])
    def test_get_metadata_from_zookeeper_with_empty_value_and_must_has_data(
        self, metadata_util: MetaDataUtil, as_object
    ):
        # Mock
        metadata_util._zookeeper_client.get_value_from_node = MagicMock(return_value="")
        metadata_util._zookeeper_data_converter.deserialize_meta_data = MagicMock()

        # Run the target function to test
        metadata = None
        try:
            metadata = metadata_util.get_metadata_from_zookeeper(path="", as_obj=as_object, must_has_data=True)
        except TypeError as e:
            if issubclass(as_object, InvalidObject):
                expected_err_msg = f"It doesn't support deserialize data as type '{as_object}' recently."
                assert str(e) == expected_err_msg, f"The error message should be same as '{expected_err_msg}'."
            else:
                assert False, "It is a valid type object so that it should NOT occur any issue or raise any exception."

        # Verify running result
        metadata_util._zookeeper_client.get_value_from_node.assert_called_once_with(path="")
        metadata_util._zookeeper_data_converter.deserialize_meta_data.assert_not_called()
        if issubclass(as_object, InvalidObject):
            assert metadata is None, "It should be None because it cannot get data finely."
        else:
            assert isinstance(metadata, as_object), f"The running result should be the *{as_object}* type instance."

    def test_get_metadata_from_zookeeper_with_empty_value_without_must_has_data(self, metadata_util: MetaDataUtil):
        # Mock
        metadata_util._zookeeper_client.get_value_from_node = MagicMock(return_value="")
        metadata_util._zookeeper_data_converter.deserialize_meta_data = MagicMock()

        # Run the target function to test
        metadata = metadata_util.get_metadata_from_zookeeper(path="", as_obj=Mock(), must_has_data=False)

        # Verify running result
        metadata_util._zookeeper_client.get_value_from_node.assert_called_once_with(path="")
        metadata_util._zookeeper_data_converter.deserialize_meta_data.assert_not_called()
        assert metadata is None, "The running result should be None object."
