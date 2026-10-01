from unittest.mock import Mock

import pytest
from dbt_common.clients.jinja import get_environment
from jinja2 import FileSystemLoader

from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.base import DatabricksRelationChangeSet
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsConfig
from dbt.adapters.databricks.relation_configs.comment import CommentConfig
from dbt.adapters.databricks.relation_configs.partitioning import PartitionedByConfig
from dbt.adapters.databricks.relation_configs.refresh import RefreshConfig
from dbt.adapters.databricks.relation_configs.row_filter import RowFilterConfig
from dbt.adapters.databricks.relation_configs.tags import TagsConfig
from dbt.adapters.databricks.relation_configs.tblproperties import TblPropertiesConfig
from tests.unit.macros.base import MacroTestBase


def capture_macro_return(context, template, macro_name, *args):
    captured = {}
    context["return"] = lambda value: captured.__setitem__("value", value)
    getattr(template.module, macro_name)(*args)
    return captured["value"]


def jinja_safe_mock(*args, **kwargs):
    mock = Mock(*args, **kwargs)
    mock.unsafe_callable = False
    mock.alters_data = False
    return mock


class TestMaterializedViewAlterTags(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "alter.sql"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/materialized_view", "macros/relations", "macros"]

    @pytest.fixture(autouse=True)
    def materialized_view_relation(self, relation):
        relation.type = DatabricksRelationType.MaterializedView

    @pytest.fixture(autouse=True)
    def desired_refresh(self, context):
        context["refresh_materialized_view"] = Mock(return_value="REFRESH MATERIALIZED VIEW")
        model_config = context["adapter"].get_config_from_model.return_value
        model_config.config = {"refresh": RefreshConfig()}
        return model_config

    def test_in_place_alter_appends_only_tag_changes(self, template_bundle, context):
        table_tags = TagsConfig(set_tags={"updated": "new"})
        column_tags = ColumnTagsConfig(
            set_column_tags={"id": {"classification": "public", "owner": "analytics"}}
        )
        configuration_changes = DatabricksRelationChangeSet(
            changes={"tags": table_tags, "column_tags": column_tags},
            requires_full_refresh=False,
        )
        get_set_tag_statements = Mock(return_value=["TABLE TAG DELTA", "COLUMN TAGS FOR ID"])
        context["get_set_tag_statements"] = get_set_tag_statements

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "databricks__get_alter_materialized_view_as_sql",
            template_bundle.relation,
            configuration_changes,
            "select 1 as id",
            Mock(),
            None,
            None,
        )

        assert statements == [
            "TABLE TAG DELTA",
            "COLUMN TAGS FOR ID",
            "REFRESH MATERIALIZED VIEW",
        ]
        get_set_tag_statements.assert_called_once_with(
            template_bundle.relation, table_tags.set_tags, column_tags
        )

    @pytest.mark.parametrize(
        "refresh, expects_refresh",
        [
            (RefreshConfig(), True),
            (RefreshConfig(cron="0 0 * * * ? *"), True),
            (RefreshConfig(every="4 WEEKS"), False),
            (RefreshConfig(on_update=True), False),
        ],
    )
    def test_in_place_alter_refreshes_unless_auto_refreshed(
        self, template_bundle, context, desired_refresh, refresh, expects_refresh
    ):
        desired_refresh.config = {"refresh": refresh}
        configuration_changes = DatabricksRelationChangeSet(
            changes={"tags": TagsConfig(set_tags={"updated": "new"})},
            requires_full_refresh=False,
        )
        context["get_set_tag_statements"] = Mock(return_value=["TABLE TAG DELTA"])

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "databricks__get_alter_materialized_view_as_sql",
            template_bundle.relation,
            configuration_changes,
            "select 1 as id",
            Mock(),
            None,
            None,
        )

        expected = ["TABLE TAG DELTA"]
        if expects_refresh:
            expected.append("REFRESH MATERIALIZED VIEW")
        assert statements == expected

    def test_unrelated_alter_appends_no_tag_statements(self, template_bundle, context):
        row_filter = RowFilterConfig(is_change=True, should_unset=True)
        configuration_changes = DatabricksRelationChangeSet(
            changes={"row_filter": row_filter}, requires_full_refresh=False
        )
        context["alter_drop_row_filter"] = Mock(return_value="DROP ROW FILTER")
        context["get_set_tag_statements"] = Mock(return_value=[])

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "databricks__get_alter_materialized_view_as_sql",
            template_bundle.relation,
            configuration_changes,
            "select 1 as id",
            Mock(),
            None,
            None,
        )

        assert statements == ["DROP ROW FILTER", "REFRESH MATERIALIZED VIEW"]
        context["get_set_tag_statements"].assert_called_once_with(
            template_bundle.relation, None, None
        )

    def test_configuration_replacement_appends_full_desired_tags(
        self, template_bundle, context, config
    ):
        full_table_tags = {"unchanged": "value", "updated": "new"}
        full_column_tags = ColumnTagsConfig(
            set_column_tags={
                "id": {"classification": "public"},
                "email": {"classification": "restricted"},
            }
        )
        config["databricks_tags"] = full_table_tags
        context["adapter"].get_column_tags_from_model.return_value = full_column_tags
        context["get_replace_sql"] = Mock(return_value="REPLACE MATERIALIZED VIEW")
        context["get_set_tag_statements"] = Mock(return_value=["ALL TABLE TAGS", "ALL COLUMN TAGS"])
        configuration_changes = DatabricksRelationChangeSet(changes={}, requires_full_refresh=True)

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "databricks__get_alter_materialized_view_as_sql",
            template_bundle.relation,
            configuration_changes,
            "select 1 as id",
            Mock(),
            None,
            None,
        )

        assert statements == ["REPLACE MATERIALIZED VIEW", "ALL TABLE TAGS", "ALL COLUMN TAGS"]
        context["get_set_tag_statements"].assert_called_once_with(
            template_bundle.relation, full_table_tags, full_column_tags
        )
        context["refresh_materialized_view"].assert_not_called()


class TestStreamingTableAlterTags(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "alter.sql"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/streaming_table", "macros/relations", "macros"]

    @pytest.fixture(autouse=True)
    def streaming_table_relation(self, relation):
        relation.type = DatabricksRelationType.StreamingTable

    @staticmethod
    def structural_changes():
        return {
            "partition_by": PartitionedByConfig(partition_by=[]),
            "tblproperties": TblPropertiesConfig(tblproperties={}),
            "comment": CommentConfig(),
            "refresh": RefreshConfig(),
        }

    def test_in_place_alter_appends_only_tag_changes(self, template_bundle, context):
        table_tags = TagsConfig(set_tags={"updated": "new"})
        column_tags = ColumnTagsConfig(
            set_column_tags={"id": {"classification": "public", "owner": "analytics"}}
        )
        configuration_changes = DatabricksRelationChangeSet(
            changes={
                **self.structural_changes(),
                "tags": table_tags,
                "column_tags": column_tags,
            },
            requires_full_refresh=False,
        )
        context["liquid_clustered_cols"] = Mock(return_value="")
        context["get_set_tag_statements"] = Mock(
            return_value=["TABLE TAG DELTA", "COLUMN TAGS FOR ID"]
        )

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "databricks__get_alter_streaming_table_as_sql",
            template_bundle.relation,
            configuration_changes,
            "select stream(1 as id)",
            Mock(),
            None,
            None,
        )

        assert statements[1:] == ["TABLE TAG DELTA", "COLUMN TAGS FOR ID"]
        context["get_set_tag_statements"].assert_called_once_with(
            template_bundle.relation, table_tags.set_tags, column_tags
        )

    def test_unrelated_change_appends_no_tag_statements(self, template_bundle, context):
        configuration_changes = DatabricksRelationChangeSet(
            changes=self.structural_changes(), requires_full_refresh=False
        )
        context["liquid_clustered_cols"] = Mock(return_value="")
        context["get_set_tag_statements"] = Mock(return_value=[])

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "databricks__get_alter_streaming_table_as_sql",
            template_bundle.relation,
            configuration_changes,
            "select stream(1 as id)",
            Mock(),
            None,
            None,
        )

        assert len(statements) == 1
        assert "create or refresh streaming table" in statements[0].lower()
        context["get_set_tag_statements"].assert_called_once_with(
            template_bundle.relation, None, None
        )

    def test_configuration_replacement_appends_full_desired_tags(
        self, template_bundle, context, config
    ):
        full_table_tags = {"unchanged": "value", "updated": "new"}
        full_column_tags = ColumnTagsConfig(
            set_column_tags={
                "id": {"classification": "public"},
                "email": {"classification": "restricted"},
            }
        )
        config["databricks_tags"] = full_table_tags
        context["adapter"].get_column_tags_from_model.return_value = full_column_tags
        context["get_replace_sql"] = Mock(return_value="REPLACE STREAMING TABLE")
        context["get_set_tag_statements"] = Mock(return_value=["ALL TABLE TAGS", "ALL COLUMN TAGS"])
        configuration_changes = DatabricksRelationChangeSet(changes={}, requires_full_refresh=True)

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "databricks__get_alter_streaming_table_as_sql",
            template_bundle.relation,
            configuration_changes,
            "select stream(1 as id)",
            Mock(),
            None,
            None,
        )

        assert statements == ["REPLACE STREAMING TABLE", "ALL TABLE TAGS", "ALL COLUMN TAGS"]
        context["get_set_tag_statements"].assert_called_once_with(
            template_bundle.relation, full_table_tags, full_column_tags
        )


class DeltaLiveTableMaterializationTagsTestBase(MacroTestBase):
    @pytest.fixture
    def databricks_env(self, macro_folders_to_load):
        environment = get_environment()
        environment.loader = FileSystemLoader(
            [f"dbt/include/databricks/{folder}" for folder in macro_folders_to_load]
        )
        return environment

    @staticmethod
    def configure_execution_context(context):
        context["pre_hooks"] = []
        context["run_hooks"] = jinja_safe_mock(return_value="")
        context["execute_multiple_statements"] = jinja_safe_mock(return_value="")
        context["should_revoke"] = jinja_safe_mock(return_value=False)
        context["apply_grants"] = jinja_safe_mock(return_value="")

    @staticmethod
    def configure_build_context(context, full_refresh):
        context["sql"] = "select 1 as id"
        context["adapter"].clean_sql.return_value = context["sql"]
        context["adapter"].clean_sql.unsafe_callable = False
        context["adapter"].clean_sql.alters_data = False
        context["should_full_refresh"] = jinja_safe_mock(return_value=full_refresh)
        context["get_set_tag_statements"] = jinja_safe_mock(
            return_value=["TABLE TAGS", "COLUMN TAGS"]
        )
        context["adapter"].get_column_tags_from_model.unsafe_callable = False
        context["adapter"].get_column_tags_from_model.alters_data = False

    @staticmethod
    def existing_relation(scenario, relation_type_attribute):
        if scenario == "create":
            return None
        relation = Mock()
        setattr(relation, relation_type_attribute, scenario != "wrong_type")
        return relation


class TestMaterializedViewMaterializationTags(DeltaLiveTableMaterializationTagsTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "materialized_view.sql"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations"]

    def test_execution_only_executes_completed_build_sql(self, template_bundle, context):
        self.configure_execution_context(context)
        existing_relation = Mock(is_materialized_view=True)

        self.run_macro_raw(
            template_bundle.template,
            "dbt_macro__materialized_view_execute_build_sql",
            "REFRESH MATERIALIZED VIEW",
            existing_relation,
            template_bundle.relation,
            [],
        )

        context["execute_multiple_statements"].assert_called_once_with("REFRESH MATERIALIZED VIEW")

    @pytest.mark.parametrize("scenario", ["create", "full_refresh", "wrong_type"])
    def test_create_and_outer_replacements_apply_full_tags(
        self, template_bundle, context, config, scenario
    ):
        full_table_tags = {"owner": "analytics"}
        full_column_tags = ColumnTagsConfig(set_column_tags={"id": {"classification": "internal"}})
        config["databricks_tags"] = full_table_tags
        self.configure_build_context(context, full_refresh=scenario == "full_refresh")
        context["adapter"].get_column_tags_from_model.return_value = full_column_tags
        context["get_create_materialized_view_as_sql"] = jinja_safe_mock(
            return_value="CREATE MATERIALIZED VIEW"
        )
        context["get_replace_sql"] = jinja_safe_mock(
            return_value="CREATE OR REPLACE MATERIALIZED VIEW"
        )
        existing_relation = self.existing_relation(scenario, "is_materialized_view")

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "dbt_macro__materialized_view_get_build_sql",
            existing_relation,
            template_bundle.relation,
        )

        context["get_set_tag_statements"].assert_called_once_with(
            template_bundle.relation, full_table_tags, full_column_tags
        )
        expected_build = (
            "CREATE MATERIALIZED VIEW"
            if scenario == "create"
            else "CREATE OR REPLACE MATERIALIZED VIEW"
        )
        assert statements == [expected_build, "TABLE TAGS", "COLUMN TAGS"]


class TestStreamingTableMaterializationTags(DeltaLiveTableMaterializationTagsTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "streaming_table.sql"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations"]

    def test_execution_only_executes_completed_build_sql(self, template_bundle, context):
        self.configure_execution_context(context)
        existing_relation = Mock(is_streaming_table=True)

        self.run_macro_raw(
            template_bundle.template,
            "dbt_macro__streaming_table_execute_build_sql",
            "REFRESH STREAMING TABLE",
            existing_relation,
            template_bundle.relation,
            [],
        )

        context["execute_multiple_statements"].assert_called_once_with("REFRESH STREAMING TABLE")

    @pytest.mark.parametrize("scenario", ["create", "full_refresh", "wrong_type"])
    def test_create_and_outer_replacements_apply_full_tags(
        self, template_bundle, context, config, scenario
    ):
        full_table_tags = {"owner": "analytics"}
        full_column_tags = ColumnTagsConfig(set_column_tags={"id": {"classification": "internal"}})
        config["databricks_tags"] = full_table_tags
        self.configure_build_context(context, full_refresh=scenario == "full_refresh")
        context["adapter"].get_column_tags_from_model.return_value = full_column_tags
        context["get_create_streaming_table_as_sql"] = jinja_safe_mock(
            return_value="CREATE STREAMING TABLE"
        )
        context["get_replace_sql"] = jinja_safe_mock(
            return_value="CREATE OR REPLACE STREAMING TABLE"
        )
        existing_relation = self.existing_relation(scenario, "is_streaming_table")

        statements = capture_macro_return(
            context,
            template_bundle.template,
            "dbt_macro__streaming_table_get_build_sql",
            existing_relation,
            template_bundle.relation,
        )

        context["get_set_tag_statements"].assert_called_once_with(
            template_bundle.relation, full_table_tags, full_column_tags
        )
        expected_build = (
            "CREATE STREAMING TABLE"
            if scenario == "create"
            else "CREATE OR REPLACE STREAMING TABLE"
        )
        assert statements == [expected_build, "TABLE TAGS", "COLUMN TAGS"]
