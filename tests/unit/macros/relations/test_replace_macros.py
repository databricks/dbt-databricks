from unittest.mock import Mock

import pytest
from dbt.adapters.cache import RelationsCache
from dbt.adapters.spark.impl import KEY_TABLE_OWNER
from dbt_common.exceptions.macros import MacroReturn

from dbt.adapters.databricks.relation import DatabricksRelation
from tests.unit.macros.base import MacroTestBase


class TestGetReplaceSql(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "replace.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros"]

    @pytest.fixture
    def default_context(self) -> dict:
        context = MacroTestBase.default_context.__wrapped__(self)

        def macro_return(value):
            raise MacroReturn(value)

        context["return"] = macro_return
        context["adapter"].resolve_file_format = Mock(return_value="delta")
        context["statement"] = lambda name=None, caller=None: caller()
        context["make_staging_relation"] = lambda relation, type=None: DatabricksRelation.create(
            identifier=f"{relation.identifier}__dbt_stg", type=type
        )
        context["drop_relation_if_exists"] = Mock(return_value="")
        context["create_backup"] = Mock(return_value="")
        context["get_create_sql"] = lambda relation, sql: f"create {relation.type}"
        context["get_drop_sql"] = lambda relation: f"drop {relation.type}"
        context["get_rename_sql"] = lambda relation, new_name: f"rename {relation.identifier}"
        context["get_drop_backup_sql"] = lambda relation: "drop backup"
        context["get_replace_view_sql"] = lambda relation, sql: "create or replace view"
        context["get_replace_materialized_view_sql"] = (
            lambda relation, sql: "create or replace materialized view"
        )
        return context

    def get_replace_sql(self, template_bundle, existing_relation, target_type):
        target_relation = existing_relation.incorporate(type=target_type)
        with pytest.raises(MacroReturn) as result:
            self.run_macro_raw(
                template_bundle.template,
                "databricks__get_replace_sql",
                existing_relation,
                target_relation,
                "select 1",
            )
        return result.value.value

    @pytest.mark.parametrize("safe_replace", [False, True])
    def test_hive_metastore_table_to_view_does_not_rename_table(
        self, template_bundle, config, context, safe_replace
    ):
        config["use_safer_relation_operations"] = safe_replace
        existing = DatabricksRelation.create(
            database="hive_metastore", schema="schema", identifier="model", type="table"
        )

        statements = self.get_replace_sql(template_bundle, existing, "view")

        assert statements == ["drop table", "rename model__dbt_stg"]
        context["create_backup"].assert_not_called()
        context["adapter"].cache_dropped.assert_called_once_with(existing)

    @pytest.mark.parametrize("safe_replace", [False, True])
    def test_unity_catalog_table_to_view_backs_up_table(
        self, template_bundle, config, context, safe_replace
    ):
        config["use_safer_relation_operations"] = safe_replace
        existing = DatabricksRelation.create(
            database="main", schema="schema", identifier="model", type="table"
        )

        statements = self.get_replace_sql(template_bundle, existing, "view")

        assert statements == ["rename model__dbt_stg", "drop backup"]
        context["create_backup"].assert_called_once_with(existing)

    @pytest.mark.parametrize("target_type", ["materialized_view", "streaming_table"])
    def test_unity_catalog_table_to_non_renamable_backs_up_table(
        self, template_bundle, context, target_type
    ):
        existing = DatabricksRelation.create(
            database="main", schema="schema", identifier="model", type="table"
        )

        statements = self.get_replace_sql(template_bundle, existing, target_type)

        assert statements == [f"create {target_type}", "drop backup"]
        context["create_backup"].assert_called_once_with(existing)

    @pytest.mark.parametrize("safe_replace", [False, True])
    @pytest.mark.parametrize("database", ["hive_metastore", "main"])
    @pytest.mark.parametrize("file_format", ["delta", "parquet"])
    def test_view_to_view_uses_create_or_replace(
        self, template_bundle, config, context, database, safe_replace, file_format
    ):
        config["use_safer_relation_operations"] = safe_replace
        context["adapter"].resolve_file_format.return_value = file_format
        existing = DatabricksRelation.create(
            database=database, schema="schema", identifier="model", type="view"
        )

        assert self.get_replace_sql(template_bundle, existing, "view") == "create or replace view"
        context["create_backup"].assert_not_called()
        context["drop_relation_if_exists"].assert_not_called()


def model_relation(type, **kwargs):
    return DatabricksRelation.create(
        database="main", schema="schema", identifier="model", type=type, **kwargs
    )


def add_after_model(cache, relation):
    """What dbt-core does for a materialization's returned relations after the model."""
    cache.add(relation.incorporate(dbt_created=True))


def cached_model(cache):
    return next((r for r in cache.get_relations("main", "schema") if r.identifier == "model"), None)


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

    def run_helper(self, template_bundle, context, cache, target):
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

    def test_missing_entry_matches_dbt_core(self, template_bundle, context):
        target = model_relation("view")
        cache = self.make_cache()
        dbt_core_only = self.make_cache()

        self.run_helper(template_bundle, context, cache, target)
        assert cached_model(cache) == target.incorporate(dbt_created=True)

        add_after_model(cache, target)
        add_after_model(dbt_core_only, target)
        assert cached_model(cache) == cached_model(dbt_core_only)

    def test_entry_of_another_type_becomes_dbt_core_entry(self, template_bundle, context):
        target = model_relation("view")
        cache = self.make_cache(model_relation("materialized_view"))

        self.run_helper(template_bundle, context, cache, target)
        assert cached_model(cache) == target.incorporate(dbt_created=True)

        add_after_model(cache, target)
        assert cached_model(cache) == target.incorporate(dbt_created=True)

    def test_entry_of_same_type_is_kept(self, template_bundle, context):
        existing = model_relation("materialized_view", metadata={KEY_TABLE_OWNER: "owner"})
        target = model_relation("materialized_view")
        cache = self.make_cache(existing)
        dbt_core_only = self.make_cache(existing)

        self.run_helper(template_bundle, context, cache, target)
        assert cached_model(cache) == existing

        add_after_model(cache, target)
        add_after_model(dbt_core_only, target)
        assert cached_model(cache) == cached_model(dbt_core_only) == existing
