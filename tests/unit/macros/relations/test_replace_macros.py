import pytest
from dbt.adapters.cache import RelationsCache
from dbt.adapters.spark.impl import KEY_TABLE_OWNER

from dbt.adapters.databricks.relation import DatabricksRelation
from tests.unit.macros.base import MacroTestBase


def model_relation(type, **kwargs):
    return DatabricksRelation.create(
        database="main", schema="schema", identifier="model", type=type, **kwargs
    )


def add_after_model(cache, relation):
    """What dbt-core does for a materialization's returned relations after the model."""
    cache.add(relation.incorporate(dbt_created=True))


def cached_model(cache):
    return next(r for r in cache.get_relations("main", "schema") if r.identifier == "model")


class TestCacheReplacedRelation(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replace.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    def make_cache(self, entry=None):
        cache = RelationsCache()
        cache.add_schema("main", "schema")
        if entry is not None:
            cache.add(entry)
        return cache

    def run_with_cache(self, template_bundle, context, cache, target):
        adapter = context["adapter"]
        adapter.get_relation = lambda database, schema, identifier: next(
            (
                r
                for r in cache.get_relations(database, schema)
                if r.matches(database, schema, identifier)
            ),
            None,
        )
        adapter.cache_added = cache.add
        adapter.cache_dropped = cache.drop
        self.run_macro_raw(template_bundle.template, "cache_replaced_relation", target)
        add_after_model(cache, target)

    def test_missing_entry_matches_dbt_core(self, template_bundle, context):
        target = model_relation("view")
        cache = self.make_cache()
        dbt_core_only = self.make_cache()

        self.run_with_cache(template_bundle, context, cache, target)
        add_after_model(dbt_core_only, target)

        assert cached_model(cache) == cached_model(dbt_core_only)

    def test_entry_of_another_type_becomes_dbt_core_entry(self, template_bundle, context):
        target = model_relation("view")
        cache = self.make_cache(model_relation("materialized_view"))

        self.run_with_cache(template_bundle, context, cache, target)

        assert cached_model(cache) == target.incorporate(dbt_created=True)

    def test_entry_of_same_type_is_kept(self, template_bundle, context):
        existing = model_relation("materialized_view", metadata={KEY_TABLE_OWNER: "owner"})
        cache = self.make_cache(existing)
        dbt_core_only = self.make_cache(existing)

        self.run_with_cache(template_bundle, context, cache, model_relation("materialized_view"))
        add_after_model(dbt_core_only, model_relation("materialized_view"))

        assert cached_model(cache) == cached_model(dbt_core_only) == existing
