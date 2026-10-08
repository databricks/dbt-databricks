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

    def cache_calls(self, template_bundle, context, cached, target):
        context["adapter"].get_relation.return_value = cached
        self.run_macro_raw(template_bundle.template, "cache_replaced_relation", target)
        return [c for c in context["adapter"].mock_calls if c[0].startswith("cache_")]

    def test_replaces_entry_of_another_type(self, template_bundle, context):
        cached = model_relation("materialized_view")
        target = model_relation("view")

        assert self.cache_calls(template_bundle, context, cached, target) == [
            call.cache_dropped(cached),
            call.cache_added(target),
        ]

    def test_adds_missing_entry(self, template_bundle, context):
        target = model_relation("view")

        assert self.cache_calls(template_bundle, context, None, target) == [
            call.cache_added(target)
        ]

    def test_keeps_entry_of_same_type(self, template_bundle, context):
        cached = model_relation("materialized_view")
        target = model_relation("materialized_view")

        assert self.cache_calls(template_bundle, context, cached, target) == []
