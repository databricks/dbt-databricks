from unittest.mock import call

import pytest

from dbt.adapters.databricks.relation import DatabricksRelation
from tests.unit.macros.base import MacroTestBase


def model_relation(type):
    return DatabricksRelation.create(
        database="main", schema="schema", identifier="model", type=type
    )


class TestCacheReplacedRelation(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replace.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    def cache_calls(self, context):
        return [c for c in context["adapter"].mock_calls if c[0].startswith("cache_")]

    def test_replaces_existing_entry_with_target(self, template_bundle, context):
        existing = model_relation("materialized_view")
        target = model_relation("view")

        self.run_macro_raw(template_bundle.template, "cache_replaced_relation", existing, target)

        assert self.cache_calls(context) == [
            call.cache_dropped(existing),
            call.cache_added(target),
        ]

    def test_adds_target_without_existing(self, template_bundle, context):
        target = model_relation("materialized_view")

        self.run_macro_raw(template_bundle.template, "cache_replaced_relation", None, target)

        assert self.cache_calls(context) == [call.cache_added(target)]
