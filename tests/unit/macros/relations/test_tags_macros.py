import pytest

from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsConfig
from tests.unit.macros.base import MacroTestBase


class TestTagsMacros(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "tags.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations", "macros/relations/components", "macros"]

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return ["column_tags.sql"]

    def test_macros_fetch_tags_sql(self, template_bundle):
        sql = self.render_bundle(template_bundle, "fetch_tags_sql")
        expected = self.clean_sql(
            "SELECT tag_name, tag_value "
            "FROM `system`.`information_schema`.`table_tags` "
            "WHERE catalog_name = 'some_database'"
            " AND schema_name = 'some_schema' AND table_name = 'some_table'"
        )
        assert sql == expected

    def test_get_set_tag_statements_returns_table_delta_and_changed_columns(
        self, template_bundle, context
    ):
        template_bundle.relation.type = DatabricksRelationType.MaterializedView
        column_tags = ColumnTagsConfig(
            set_column_tags={
                "id": {"classification": "public", "owner": "analytics"},
                "email": {"classification": "restricted"},
            }
        )
        captured = {}
        context["return"] = lambda value: captured.__setitem__("statements", value)

        self.run_macro_raw(
            template_bundle.template,
            "get_set_tag_statements",
            template_bundle.relation,
            {"updated": "new"},
            column_tags,
        )

        statements = [self.clean_sql(statement) for statement in captured["statements"]]
        assert statements == [
            self.clean_sql(
                "ALTER MATERIALIZED VIEW `some_database`.`some_schema`.`some_table` "
                "SET TAGS ('updated' = 'new')"
            ),
            self.clean_sql(
                "ALTER MATERIALIZED VIEW `some_database`.`some_schema`.`some_table` "
                "ALTER COLUMN `id` SET TAGS "
                "('classification' = 'public', 'owner' = 'analytics')"
            ),
            self.clean_sql(
                "ALTER MATERIALIZED VIEW `some_database`.`some_schema`.`some_table` "
                "ALTER COLUMN `email` SET TAGS ('classification' = 'restricted')"
            ),
        ]

    def test_get_set_tag_statements_noops_without_tag_changes(self, template_bundle, context):
        captured = {}
        context["return"] = lambda value: captured.__setitem__("statements", value)

        self.run_macro_raw(
            template_bundle.template,
            "get_set_tag_statements",
            template_bundle.relation,
            None,
            None,
        )

        assert captured["statements"] == []

    def test_macros_alter_set_tags(self, template_bundle):
        template_bundle.relation.type = DatabricksRelationType.View
        sql = self.render_bundle(template_bundle, "alter_set_tags", {"a": "valA", "b": "valB"})
        expected = self.clean_sql(
            "ALTER view `some_database`.`some_schema`.`some_table` "
            "SET TAGS ( 'a' = 'valA', 'b' = 'valB' )"
        )

        assert sql == expected
