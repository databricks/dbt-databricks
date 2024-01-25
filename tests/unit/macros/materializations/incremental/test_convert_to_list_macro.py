import pytest

from tests.unit.macros.base import MacroTestBase


class TestConvertToListMacro(MacroTestBase):
    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "convert_to_list.sql"

    @pytest.mark.parametrize(
        "func_input,expected", [(["a"], ["a"]), (None, []), ([], [])]
    )
    def test_convert_to_list_default_empty_list(self, template, func_input, expected):
        actual = self.run_macro(template, "convert_to_list", func_input, [])
        assert actual == f"{expected}"
