import re

import pytest

from tests.unit.macros.base import MacroTestBase


class TestSplitPartMacro(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "split_part.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/utils", "macros"]

    def render(self, template, *args) -> str:
        sql = self.run_macro_raw(template, "databricks__split_part", *args)
        return re.sub(r"\s+", " ", sql).strip()

    @pytest.mark.parametrize(
        "part_number, index",
        [(0, "0"), (1, "0"), (3, "2")],
    )
    def test_non_negative_part_splits_on_literal_delimiter(self, template, part_number, index):
        sql = self.render(template, "col", "'x'", part_number)

        assert sql == f"get(split(col, concat('\\\\Q', 'x', '\\\\E')), {index})"

    def test_negative_part_counts_from_end_of_split(self, template):
        sql = self.render(template, "col", "'--'", -1)

        assert sql == (
            "get(split(col, concat('\\\\Q', '--', '\\\\E')),"
            " size(split(col, concat('\\\\Q', '--', '\\\\E'))) + -1)"
        )
