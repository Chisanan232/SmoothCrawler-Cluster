import traceback
from abc import ABCMeta
from datetime import datetime
from enum import Enum
from typing import Callable


class _MetaDataTest(metaclass=ABCMeta):
    @classmethod
    def _run_property_test(
        cls,
        setting_func: Callable,
        getting_func: Callable,
        valid_value,
        invalid_1_value=None,
        invalid_2_value=None,
    ) -> None:
        """
        Test for getting and setting the property value. It also try to operate the property with invalid value to test
        about it should NOT work finely without any issue.

        :param setting_func: The function which would set value to the property.
                                         The function would be like below: (for example with property *fail_backup*)

                                         .. code_block: python

                                            def set_func(state: State, value: List[str]) -> None:
                                                state.fail_backup = state

        :param getting_func: The function which would get value by the property.
                                         The function would be like below: (for example with property *fail_backup*)

                                         .. code_block: python

                                            def get_func(state: State) -> List[str]:
                                                return state.fail_backup

        :param valid_value: The valid value which could be set to the property.
        :param invalid_1_value: The invalid value which would raise an exception if set it to the property.
        :param invalid_2_value: The second one invalid value.
        :return: None
        """

        assert getting_func() is None, "Default initial value should be None value."

        # Set value with normal value.
        test_cnt = valid_value
        try:
            setting_func(test_cnt)
        except Exception:
            assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
        else:
            assert True, "It works finely."
            if isinstance(test_cnt, Enum):
                assert getting_func() == test_cnt.value, "The value should be same as it set."
            elif isinstance(test_cnt, datetime):
                assert getting_func() == test_cnt.strftime("%Y-%m-%d %H:%M:%S"), "The value should be same as it set."
            elif isinstance(getattr(test_cnt, "_fields", None), tuple) is True:
                # Testing for namedtuple type object
                fields = getattr(test_cnt, "_fields")
                current_values = getting_func()
                for f in fields:
                    assert getattr(test_cnt, f) == current_values[f], "The value should be same as it set."
            elif isinstance(test_cnt, list) and False not in map(
                lambda one_test: isinstance(getattr(one_test, "_fields", None), tuple) is True, test_cnt
            ):
                # Testing for list type value with namedtuple type elements
                fields = None
                current_values = getting_func()
                for one, value in zip(test_cnt, current_values):
                    if fields is None:
                        fields = getattr(one, "_fields")
                    for f in fields:
                        assert getattr(one, f) == value[f], "The value should be same as it set."
            else:
                assert getting_func() == test_cnt, "The value should be same as it set."

        # Set value with normal value.
        if invalid_1_value:
            test_cnt = invalid_1_value
            try:
                setting_func(test_cnt)
            except Exception:
                assert True, "It works finely."
            else:
                assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"

        # Set value with normal value.
        if invalid_2_value:
            test_cnt = invalid_2_value
            try:
                setting_func(test_cnt)
            except Exception:
                assert True, "It works finely."
            else:
                assert False, f"It should work finely without any issue.\n The error is: {traceback.format_exc()}"
