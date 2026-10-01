from unittest.mock import Mock

import pytest
from dbt_common.clients.jinja import MaterializationExtension
from dbt_common.exceptions.macros import MacroReturn

from dbt.adapters.databricks.relation import DatabricksRelation
from tests.unit.macros.base import MacroTestBase


class TestRelationShouldBeAltered(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self):
        return "materializations/view.sql"

    @pytest.fixture
    def databricks_env(self, macro_folders_to_load):
        databricks_env = MacroTestBase.databricks_env.__wrapped__(self, macro_folders_to_load)
        databricks_env.add_extension(MaterializationExtension)
        return databricks_env

    @pytest.fixture
    def default_context(self):
        context = MacroTestBase.default_context.__wrapped__(self)

        def macro_return(value):
            raise MacroReturn(value)

        def compiler_error(message):
            raise ValueError(message)

        context["should_full_refresh"] = Mock(return_value=False)
        context["return"] = macro_return
        context["exceptions"].raise_compiler_error.side_effect = compiler_error
        return context

    @pytest.mark.parametrize(
        "existing_type,target_type,update_via_alter,full_refresh,expected",
        [
            pytest.param("view", "view", True, False, True, id="matching-views"),
            pytest.param(
                "metric_view", "metric_view", True, False, True, id="matching-metric-views"
            ),
            pytest.param("metric_view", "view", True, False, False, id="metric-view-to-view"),
            pytest.param("view", "metric_view", True, False, False, id="view-to-metric-view"),
            pytest.param("table", "view", True, False, False, id="table-to-view"),
            pytest.param("view", "view", False, False, False, id="alter-disabled"),
            pytest.param("view", "view", True, True, False, id="full-refresh"),
        ],
    )
    def test_alter_eligibility(
        self,
        template_bundle,
        config,
        context,
        existing_type,
        target_type,
        update_via_alter,
        full_refresh,
        expected,
    ):
        config["view_update_via_alter"] = update_via_alter
        context["should_full_refresh"].return_value = full_refresh
        existing = DatabricksRelation.create(
            database="catalog", schema="schema", identifier="model", type=existing_type
        )
        target = existing.incorporate(type=target_type)
        with pytest.raises(MacroReturn) as result:
            self.run_macro_raw(
                template_bundle.template, "relation_should_be_altered", existing, target
            )
        assert result.value.value is expected

    def test_hive_metastore_alter_rejected(self, template_bundle, config):
        config["view_update_via_alter"] = True
        relation = DatabricksRelation.create(
            database="hive_metastore", schema="schema", identifier="model", type="view"
        )
        with pytest.raises(ValueError, match="Cannot update a view in the Hive metastore"):
            self.run_macro_raw(
                template_bundle.template, "relation_should_be_altered", relation, relation
            )


class TestCreateViewAs(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "create.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/view"]

    def render_create_view_as(self, template_bundle, sql="select 1"):
        return self.run_macro(
            template_bundle.template,
            "databricks__create_view_as",
            template_bundle.relation,
            sql,
        )

    def test_macros_create_view_as_tblproperties(self, config, template_bundle):
        config["tblproperties"] = {"tblproperties_to_view": "true"}
        template_bundle.context["get_columns_in_query"] = Mock(return_value=[])
        template_bundle.context["column_mask_exists"] = Mock(return_value=False)
        template_bundle.context["column_tags_exist"] = Mock(return_value=False)
        template_bundle.context["row_filter_exists"] = Mock(return_value=False)

        sql = self.render_create_view_as(template_bundle)
        expected = (
            f"create or replace view {template_bundle.relation.render()} "
            "tblproperties ('tblproperties_to_view' = 'true') as (select 1)"
        )

        assert sql == expected


class TestAlterView(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "alter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros", "macros/relations/view"]

    @pytest.fixture(autouse=True, scope="function")
    def mocks(self, context):
        context["apply_tags"] = Mock()
        context["apply_tblproperties"] = Mock()
        context["alter_query"] = Mock()
        context["alter_column_comment"] = Mock()
        context["apply_column_tags"] = Mock()

    def render_alter_view(self, template_bundle, changes):
        return self.run_macro(
            template_bundle.template,
            "alter_view",
            template_bundle.relation,
            changes,
        )

    def test_macros__alter_view_empty_changes(self, context, template_bundle):
        self.render_alter_view(template_bundle, {})
        context["apply_tags"].assert_not_called()
        context["apply_tblproperties"].assert_not_called()
        context["alter_query"].assert_not_called()

    def test_macros__alter_view_with_tags(self, context, template_bundle):
        self.render_alter_view(template_bundle, {"tags": Mock()})
        context["apply_tags"].assert_called_once()
        context["apply_tblproperties"].assert_not_called()
        context["alter_query"].assert_not_called()

    def test_macros__alter_view_with_tblproperties(self, context, template_bundle):
        self.render_alter_view(template_bundle, {"tblproperties": Mock()})
        context["apply_tags"].assert_not_called()
        context["apply_tblproperties"].assert_called_once()
        context["alter_query"].assert_not_called()

    def test_macros__alter_view_with_query(self, context, template_bundle):
        self.render_alter_view(template_bundle, {"query": Mock()})
        context["apply_tags"].assert_not_called()
        context["apply_tblproperties"].assert_not_called()
        context["alter_query"].assert_called_once()

    def test_macros__alter_view_with_query_reapplies_column_comments(
        self, context, template_bundle
    ):
        context["config"].persist_column_docs = Mock(return_value=True)
        context["model"].columns = {"id": Mock()}
        self.render_alter_view(template_bundle, {"query": Mock()})
        context["alter_query"].assert_called_once()
        context["alter_column_comment"].assert_called_once()

    def test_macros__alter_view_with_column_tags(self, context, template_bundle):
        column_tags = Mock()
        self.render_alter_view(template_bundle, {"column_tags": column_tags})
        context["apply_tags"].assert_not_called()
        context["apply_tblproperties"].assert_not_called()
        context["alter_query"].assert_not_called()
        context["apply_column_tags"].assert_called_once_with(template_bundle.relation, column_tags)
