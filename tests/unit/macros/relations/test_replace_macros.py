from unittest.mock import Mock

import pytest
from dbt_common.exceptions.macros import MacroReturn

from dbt.adapters.databricks.relation import DatabricksRelation
from tests.unit.macros.base import MacroTestBase


def model_relation(type):
    return DatabricksRelation.create(
        database="main", schema="schema", identifier="model", type=type
    )


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
        context["get_create_sql"] = lambda relation, sql: f"create {relation.type}"
        context["get_drop_sql"] = lambda relation: f"drop {relation.type}"
        context["get_rename_sql"] = lambda relation, new_name: f"rename {relation.identifier}"
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

    @pytest.mark.parametrize(
        "existing_type", ["materialized_view", "streaming_table", "metric_view"]
    )
    def test_stage_then_replace_drops_existing_from_cache(
        self, template_bundle, context, existing_type
    ):
        existing = model_relation(existing_type)

        statements = self.get_replace_sql(template_bundle, existing, "view")

        assert statements == [f"drop {existing_type}", "rename model__dbt_stg"]
        context["adapter"].cache_dropped.assert_called_once_with(existing)

    @pytest.mark.parametrize(
        "existing_type, target_type",
        [
            ("streaming_table", "materialized_view"),
            ("materialized_view", "streaming_table"),
            ("materialized_view", "metric_view"),
        ],
    )
    def test_drop_and_create_drops_existing_from_cache(
        self, template_bundle, context, existing_type, target_type
    ):
        existing = model_relation(existing_type)

        statements = self.get_replace_sql(template_bundle, existing, target_type)

        assert statements == [f"drop {existing_type}", f"create {target_type}"]
        context["adapter"].cache_dropped.assert_called_once_with(existing)
