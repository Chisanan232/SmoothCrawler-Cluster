import traceback

import pytest

from smoothcrawler_cluster.crawler.attributes import SerialCrawlerAttribute

from ..._values import _Crawler_Name_Value


class TestSerialCrawlerAttribute:

    _og_name: str = None
    _og_id_separation: str = None

    @pytest.fixture(scope="function")
    def attribute(self) -> SerialCrawlerAttribute:
        return SerialCrawlerAttribute()

    @classmethod
    def expected_name(cls, attribute: SerialCrawlerAttribute) -> str:
        return f"{attribute._default_base_name}{attribute._default_id_separation}{attribute.current_id}"

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_name_with_empty_value_and_id_separation(self, attribute: SerialCrawlerAttribute, has_default: bool):
        # Pre-process
        attribute.has_default = has_default

        # Assert initial value
        if has_default:
            assert attribute.name == self.expected_name(
                attribute
            ), f"The crawler name should have default value '{self.expected_name(attribute)}'."
        else:
            assert attribute.name is None, "The crawler name should NOT have default value."

        # Operate setter
        attribute.id_separation = "_"
        attribute.name = ""

        # Assert running result value
        assert attribute.name == self.expected_name(
            attribute
        ), f"The crawler name should have default value '{self.expected_name(attribute)}'."

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_name_with_empty_value_without_id_separation(
        self, attribute: SerialCrawlerAttribute, has_default: bool
    ):
        # Pre-process
        attribute.has_default = has_default

        # Assert initial value
        if has_default:
            assert attribute.name == self.expected_name(
                attribute
            ), f"The crawler name should have default value '{self.expected_name(attribute)}'."
        else:
            assert attribute.name is None, "The crawler name should NOT have default value."

        try:
            # Operate setter
            attribute.name = ""
        except ValueError as e:
            # Assert running result value
            if has_default:
                assert False, f"It should NOT raise any exception. Exception: {traceback.format_exception(e)}"
            else:
                expected_err_msg = (
                    "The property *id_separation* value should NOT be empty when setting the *name* "
                    "property if it cannot have default value."
                )
                assert str(e) == expected_err_msg, f"The error message should be '{expected_err_msg}'."
        else:
            # Assert running result value
            if has_default:
                assert attribute.name == self.expected_name(
                    attribute
                ), f"The crawler name should have default value '{self.expected_name(attribute)}'."
            else:
                assert False, "It should raise exception 'ValueError'."

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_name_without_id_separation(self, attribute: SerialCrawlerAttribute, has_default: bool):
        # Pre-process
        attribute.has_default = has_default

        try:
            # Operate setter
            attribute.name = _Crawler_Name_Value
        except Exception as e:
            # Assert running result value
            assert False, f"It should NOT raise any exception. Exception: {traceback.format_exception(e)}"
        else:
            # Assert running result value
            assert (
                attribute.name == _Crawler_Name_Value
            ), f"The crawler name should have default value '{_Crawler_Name_Value}'."

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_id_separation_without_name_and_sep(self, attribute: SerialCrawlerAttribute, has_default: bool):
        # Pre-process
        attribute.has_default = has_default

        # Assert initial value
        if has_default:
            assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."
        else:
            assert attribute.id_separation is None, "The crawler ID separation should NOT have default value."

        try:
            # Operate setter
            attribute.id_separation = ""
        except ValueError as e:
            # Assert running result value
            if has_default:
                assert False, f"It should NOT raise any exception. Exception: {traceback.format_exception(e)}"
            else:
                expected_err_msg = "Argument cannot be empty when it sets *id_separation* property."
                assert str(e) == expected_err_msg, f"The error message should be '{expected_err_msg}'."
        else:
            # Assert running result value
            if has_default:
                assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."
            else:
                assert False, "It should raise exception 'ValueError'."

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_id_separation_without_name(self, attribute: SerialCrawlerAttribute, has_default: bool):
        # Pre-process
        attribute.has_default = has_default

        # Assert initial value
        if has_default:
            assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."
        else:
            assert attribute.id_separation is None, "The crawler ID separation should NOT have default value."

        try:
            # Operate setter
            attribute.id_separation = "-"
        except ValueError as e:
            # Assert running result value
            assert False, f"It should NOT raise any exception. Exception: {traceback.format_exception(e)}"
        else:
            # Assert running result value
            assert attribute.id_separation == "-", "The crawler ID separation should have default value '-'."

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_id_separation_setter_with_str(self, attribute: SerialCrawlerAttribute, has_default: bool):
        # Pre-process
        attribute.name = _Crawler_Name_Value
        attribute.has_default = has_default

        # Assert initial value
        if has_default:
            assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."
        else:
            assert attribute.id_separation is None, "The crawler ID separation should NOT have default value."

        try:
            # Operate setter
            attribute.id_separation = "-"
        except ValueError as e:
            # Assert running result value
            expected_err_msg = (
                f"This separation(s) '-' cannot parse anything from the crawler name '{_Crawler_Name_Value}'."
            )
            assert str(e) == expected_err_msg, f"The error message should be '{expected_err_msg}'."
        else:
            # Assert running result value
            assert False, "It should raise exception 'ValueError'."

        try:
            # Operate setter
            attribute.id_separation = "_"
        except Exception as e:
            # Assert running result value
            assert False, ""
        else:
            # Assert running result value
            assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_id_separation_setter_with_list(self, attribute: SerialCrawlerAttribute, has_default: bool):
        # Pre-process
        attribute.name = _Crawler_Name_Value
        attribute.has_default = has_default

        # Assert initial value
        if has_default:
            assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."
        else:
            assert attribute.id_separation is None, "The crawler ID separation should NOT have default value."

        try:
            # Operate setter
            attribute.id_separation = ["-", "_"]
        except Exception as e:
            # Assert running result value
            assert False, f"It should NOT raise any exception. Exception: {traceback.format_exception(e)}"
        else:
            # Assert running result value
            assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_id_separation_setter_with_invalid_type_data_without_name(
        self, attribute: SerialCrawlerAttribute, has_default: bool
    ):
        # Pre-process
        attribute.has_default = has_default

        # Assert initial value
        if has_default:
            assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."
        else:
            assert attribute.id_separation is None, "The crawler ID separation should NOT have default value."

        try:
            # Operate setter
            attribute.id_separation = 123
        except TypeError as e:
            # Assert running result value
            expected_err_msg = "*id_separation* property setter only accept 'str' or 'list[str]' type value."
            assert str(e) == expected_err_msg, f"The error message should be '{expected_err_msg}'."
        else:
            # Assert running result value
            assert False, "It should raise exception 'TypeError'."

    @pytest.mark.parametrize("has_default", [True, False])
    def test_prop_id_separation_setter_with_invalid_type_data_and_name(
        self, attribute: SerialCrawlerAttribute, has_default: bool
    ):
        # Pre-process
        attribute.has_default = has_default
        attribute.name = _Crawler_Name_Value

        # Assert initial value
        if has_default:
            assert attribute.id_separation == "_", "The crawler ID separation should have default value '_'."
        else:
            assert attribute.id_separation is None, "The crawler ID separation should NOT have default value."

        try:
            # Operate setter
            attribute.id_separation = 123
        except TypeError as e:
            # Assert running result value
            expected_err_msg = "*id_separation* property setter only accept 'str' or 'list[str]' type value."
            assert str(e) == expected_err_msg, f"The error message should be '{expected_err_msg}'."
        else:
            # Assert running result value
            assert False, "It should raise exception 'TypeError'."

    def test_prop_current_id(self, attribute: SerialCrawlerAttribute):
        assert attribute.current_id == "1", "The default value of property *current_id* should be '1'."

    def test_prop_next_id(self, attribute: SerialCrawlerAttribute):
        assert (
            "2" == attribute.next_id != attribute.current_id == "1"
        ), "The default value of property *current_id* should be '1' and property *next_id* should be '2'.."

    def test_prop_iter_to_next_id(self, attribute: SerialCrawlerAttribute):
        previous_id = attribute.current_id
        iter_next_id = attribute.iter_to_next_id
        assert (
            previous_id != iter_next_id and (int(iter_next_id) - int(previous_id)) == 1
        ), "The previous ID and property *iter_to_next_id* should be different and it is '1'."
        assert previous_id != attribute.current_id and attribute.current_id == iter_next_id, (
            "The previous ID and property *current_id* should be different now, current property *current_id* should "
            "be same as property *iter_to_next_id*."
        )

    def test_init(self, attribute: SerialCrawlerAttribute):
        try:
            attribute.init(name=_Crawler_Name_Value, id_separation="_")
        except Exception as e:
            assert False, f"It should NOT raise any exception. Exception: {traceback.format_exception(e)}"
        else:
            assert attribute.name == _Crawler_Name_Value, f"The crawler name should be '{_Crawler_Name_Value}'."
            assert attribute.id_separation == "_", "The crawler ID separation should be '_'."

    def test_init_with_empty_value(self, attribute: SerialCrawlerAttribute):
        try:
            attribute.init(name="", id_separation="")
        except Exception as e:
            assert False, f"It should NOT raise any exception. Exception: {traceback.format_exception(e)}"
        else:
            assert attribute.name == _Crawler_Name_Value, f"The crawler name should be '{_Crawler_Name_Value}'."
            assert attribute.id_separation == "_", "The crawler ID separation should be '_'."

    def test_init_with_incorrect_value(self, attribute: SerialCrawlerAttribute):
        try:
            attribute.init(name=_Crawler_Name_Value, id_separation="-")
        except Exception as e:
            expected_err_msg = (
                f"This separation(s) '-' cannot parse anything from the crawler name '{_Crawler_Name_Value}'."
            )
            assert str(e) == expected_err_msg, f"The error message should be '{expected_err_msg}'."
        else:
            assert False, "It should raise exception 'Exception'."
