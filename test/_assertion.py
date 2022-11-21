from typing import Any, AnyStr
from enum import Enum
import sys
import re


class WorkingTime(Enum):
    AtInitial = "In initialing process"
    AfterUpdateInInitial = "After updating in initial process"


_python_version = sys.version_info
if (_python_version[0], _python_version[1]) >= (3, 10):
    def ValueFormatAssertion(target: str, regex: re.Pattern[AnyStr]) -> None:
        assert target is not None, "The path value should not be None."

        _search_char_result = re.search(regex, str(target))
        assert _search_char_result is not None, f"Its format is not correct. It should be like '{regex}'."
elif (3, 6) < (_python_version[0], _python_version[1]) < (3, 10):
    def ValueFormatAssertion(target: str, regex: re.Pattern) -> None:
        assert target is not None, "The path value should not be None."

        _search_char_result = re.search(regex, str(target))
        assert _search_char_result is not None, f"Its format is not correct. It should be like '{regex}'."
else:
    def ValueFormatAssertion(target: str, regex) -> None:
        assert target is not None, "The path value should not be None."

        _search_char_result = re.search(regex, str(target))
        assert _search_char_result is not None, f"Its format is not correct. It should be like '{regex}'."


def ObjectIsNoneOrNotAssertion(working_time: WorkingTime, uit_obj: Any, is_none: bool = False) -> None:
    if is_none is True:
        assert uit_obj is None, f"{working_time.value}, object *{uit_obj.__class__.__name__}* should be None."
    else:
        assert uit_obj is not None, f"{working_time.value}, object *{uit_obj.__class__.__name__}* should NOT be None."


def MetaDataValueAssertion(working_time: WorkingTime, uit_obj: Any, metadata: str, expected_value: Any = None, is_none: bool = False) -> None:
    if expected_value is None:
        if is_none is True:
            _assertion = f"{working_time.value}, meta data *{uit_obj.__class__.__name__}.{metadata}* should be None."
            assert getattr(uit_obj, metadata) is None, _assertion
        else:
            _assertion = f"{working_time.value}, meta data *{uit_obj.__class__.__name__}.{metadata}* should not be None."
            assert getattr(uit_obj, metadata) is not None, _assertion
    else:
        _metadata_value = getattr(uit_obj, metadata)
        _assertion = f"{working_time.value}, meta data *{uit_obj.__class__.__name__}.{metadata}* should be {expected_value}, but it got {_metadata_value}."
        assert _metadata_value == expected_value, _assertion


def ListSizeAssertion(working_time: WorkingTime, uit_obj: Any, metadata: str, expected_value: int) -> None:
    _metadata_value = getattr(uit_obj, metadata)
    assert len(_metadata_value) == expected_value, f"{working_time.value}, meta data *{uit_obj.__class__.__name__}.{metadata}* list size should be " \
                                                   f"{expected_value}, but it got {len(_metadata_value)} (list detail: {_metadata_value})."
