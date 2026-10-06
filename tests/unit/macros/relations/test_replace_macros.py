from unittest.mock import Mock

import pytest
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

    def test_hive_metastore_view_to_view_with_safe_replace_backs_up_view(
        self, template_bundle, config, context
    ):
        config["use_safer_relation_operations"] = True
        existing = DatabricksRelation.create(
            database="hive_metastore", schema="schema", identifier="model", type="view"
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

    @pytest.mark.parametrize("database", ["hive_metastore", "main"])
    def test_view_to_view_without_safe_replace_uses_create_or_replace(
        self, template_bundle, context, database
    ):
        existing = DatabricksRelation.create(
            database=database, schema="schema", identifier="model", type="view"
        )

        assert self.get_replace_sql(template_bundle, existing, "view") == "create or replace view"
        context["create_backup"].assert_not_called()
