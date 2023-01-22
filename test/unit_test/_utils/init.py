from smoothcrawler_cluster._utils import parse_timer
import pytest
import re
import traceback


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
                assert "It only supports 's' (seconds), 'm' (minutes) or 'h' (hours) setting value." in str(e), \
                    "The error message is not correct."
            else:
                assert isinstance(e, ValueError), "It should raise a 'ValueError' exception."
                assert f"Invalid value {timer[:-1]}. It should be an integer format value." in str(e), \
                    "The error message is not correct."

