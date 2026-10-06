import pytest

from tests.unit.macros.base import MacroTestBase


class TestSkipOnEmptySourceIsSafe(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "skip_on_empty_source.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    @pytest.mark.parametrize(
        "strategy, on_schema_change, extra_config, expected",
        [
            ("merge", "ignore", {}, True),
            ("append", "ignore", {}, True),
            ("delete+insert", "ignore", {}, True),
            ("merge", "ignore", {"not_matched_by_source_action": "delete"}, False),
            ("insert_overwrite", "ignore", {"partition_by": ["a"]}, False),
            ("replace_where", "ignore", {}, False),
            ("microbatch", "ignore", {}, False),
            ("merge", "fail", {}, False),
            ("merge", "append_new_columns", {}, False),
            ("merge", "sync_all_columns", {}, False),
        ],
    )
    def test_skip_on_empty_source_is_safe(
        self, template, config, strategy, on_schema_change, extra_config, expected
    ):
        config.update(extra_config)
        result = self.run_macro_raw(
            template, "skip_on_empty_source_is_safe", strategy, on_schema_change
        )
        assert result.strip() == str(expected)
