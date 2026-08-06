"""Macro tests for MV query schema-drift detection (issue #1359)."""

import pytest

from dbt.adapters.databricks.column import DatabricksColumn
from tests.unit.macros.base import MacroTestBase


class TestDltInferredQuerySchemaChanged(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "schema_drift.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations"]

    def detect(self, template_bundle, existing: list[str], inferred: list[str]) -> bool:
        """Run the macro with the two column-name lists the adapter would return.

        The macro yields its verdict via `do return(...)`, which renders as empty text,
        so capture the value the harness's `return` hook receives.
        """
        template_bundle.context["adapter"].get_columns_in_relation = lambda relation: [
            DatabricksColumn.create(name, "string") for name in existing
        ]
        template_bundle.context["get_columns_in_query"] = lambda sql: inferred
        returned: list[bool] = []
        template_bundle.context["return"] = returned.append
        self.run_macro_raw(
            template_bundle.template,
            "dlt_inferred_query_schema_changed",
            template_bundle.relation,
            "select * from upstream",
        )
        assert len(returned) == 1, f"macro returned {len(returned)} values, expected 1"
        return returned[0]

    def test_same_names_same_order(self, template_bundle):
        assert self.detect(template_bundle, ["id", "name"], ["id", "name"]) is False

    def test_case_insensitive_name_match(self, template_bundle):
        assert self.detect(template_bundle, ["ID", "Name"], ["id", "name"]) is False

    def test_added_column(self, template_bundle):
        assert self.detect(template_bundle, ["id", "name"], ["id", "name", "new_column"]) is True

    def test_removed_column(self, template_bundle):
        assert self.detect(template_bundle, ["id", "name", "gone"], ["id", "name"]) is True

    def test_reordered_columns(self, template_bundle):
        assert self.detect(template_bundle, ["id", "name"], ["name", "id"]) is True

    def test_empty_inferred_columns(self, template_bundle):
        assert self.detect(template_bundle, ["id"], []) is True

    def test_both_empty(self, template_bundle):
        assert self.detect(template_bundle, [], []) is False
