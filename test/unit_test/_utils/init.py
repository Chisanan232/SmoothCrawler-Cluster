from smoothcrawler_cluster._utils import parse_timer, MetaDataUtil
import traceback
import pytest
import re


class TestInitModule:

    @pytest.mark.parametrize("timer", ["2s", "2.5s", "2m", "2.5m", "2h", "2.5h"])
    def test_parse_timer_with_valid_value(self, timer: str):
        _is_float: bool = False
        if "." in timer:
            _is_float = True

        try:
            _timer = parse_timer(timer=timer)
        except:
            assert False, f"It should parse the timer value correctly. \n {traceback.format_exc()}"
        else:
            if _is_float is True:
                assert type(_timer) is float, "Its data type should be 'float'."
            else:
                assert type(_timer) is int, "Its data type should be 'int'."
            _timer_unit = timer[-1]
            if _timer_unit == "s":
                _original_timer_val = _timer
            elif _timer_unit == "m":
                _original_timer_val = float(_timer / 60)
            elif _timer_unit == "h":
                _original_timer_val = float(_timer / 60 / 60)
            else:
                assert False, "It has issue in testing implement, please let developer to check it."
            if ".0" in str(_original_timer_val):
                _original_timer_val = int(_original_timer_val)
            assert str(_original_timer_val) in timer, f"The parsed value must be in the timer '{timer}'."

    @pytest.mark.parametrize("timer", ["2i", "2.6i", "test", "test_s"])
    def test_parse_timer_with_invalid_value(self, timer: str):
        try:
            parse_timer(timer=timer)
        except Exception as e:
            _timer_chksum = re.search(r"[0-9]", timer)
            if _timer_chksum is not None:
                assert type(e) is ValueError, "It should raise a 'ValueError' exception."
                assert "It only supports 's' (seconds), 'm' (minutes) or 'h' (hours) setting value." in str(e), "The error message is not correct."
            else:
                assert type(e) is ValueError, "It should raise a 'ValueError' exception."
                assert f"Invalid value {timer[:-1]}. It should be an integer format value." in str(e), "The error message is not correct."

    def test__value_is_not_empty(self):
        _is_not_none = MetaDataUtil._value_is_not_empty(_value=None)
        assert _is_not_none is False, "It should be 'False' because it is None value."

        _is_not_none = MetaDataUtil._value_is_not_empty(_value="")
        assert _is_not_none is False, "It should be 'False' because it is empty string value."

        _is_not_none = MetaDataUtil._value_is_not_empty(_value="t")
        assert _is_not_none is True, "It should be 'True' because it is not empty string value."

