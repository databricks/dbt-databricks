import pytest

from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.relation import DatabricksRelationType
from dbt.adapters.databricks.relation_configs.column_tags import ColumnTagsConfig
from tests.unit.macros.base import MacroTestBase


class TestColumnTagsMacros(MacroTestBase):
    @pytest.fixture
    def template_name(self) -> str:
        return "column_tags.sql"

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/components", "macros/relations", "macros"]

    @pytest.fixture
    def passthrough_statement(self, template_bundle):
        """Make `{% call statement('main') %}…{% endcall %}` render its body.

        The base mock returns the statement label and discards the body; for the
        apply_* wrappers we need the inner ALTER SQL so we can assert on it. Also
        force the UC path (apply_column_tags raises on hive_metastore).
        """
        template_bundle.context["statement"] = lambda label, fetch_result=False, caller=None: (
            caller() if caller else label
        )
        template_bundle.relation.is_hive_metastore = lambda: False
        return template_bundle

    def test_alter_set_column_tags_table(self, template_bundle):
        sql = self.render_bundle(
            template_bundle, "alter_set_column_tags", "email", {"pii": "true", "team": "growth"}
        )
        assert "alter table `some_database`.`some_schema`.`some_table`" in sql
        assert "alter column `email` set tags ('pii' = 'true', 'team' = 'growth')" in sql

    def test_alter_set_column_tags_view_uses_alter_table(self, template_bundle):
        # ALTER VIEW cannot set column tags, so the macro must emit ALTER TABLE for views.
        template_bundle.relation.type = DatabricksRelationType.View
        sql = self.render_bundle(template_bundle, "alter_set_column_tags", "email", {"pii": "true"})
        assert "alter table" in sql
        assert "alter view" not in sql
        assert "alter column `email` set tags ('pii' = 'true')" in sql

    def test_apply_column_tags_applies_each_column(self, passthrough_statement):
        config = ColumnTagsConfig(
            set_column_tags={"email": {"pii": "true"}, "ssn": {"pii": "true"}}
        )
        sql = self.render_bundle(passthrough_statement, "apply_column_tags", config)
        assert "alter column `email` set tags ('pii' = 'true')" in sql
        assert "alter column `ssn` set tags ('pii' = 'true')" in sql

    def test_apply_column_tags_noop_when_empty(self, passthrough_statement):
        # Idempotency safety net: when get_diff returns an empty/None changeset, the
        # {% if column_tags %} gate skips apply entirely; and even if apply_column_tags
        # is reached with nothing to set, it must emit no ALTER. This pins the latter.
        config = ColumnTagsConfig(set_column_tags={})
        sql = self.render_bundle(passthrough_statement, "apply_column_tags", config)
        assert "alter" not in sql
        assert "set tags" not in sql

    def test_alter_unset_column_tags_table(self, template_bundle):
        sql = self.render_bundle(
            template_bundle, "alter_unset_column_tags", "email", ["pii", "team"]
        )
        assert "alter table `some_database`.`some_schema`.`some_table`" in sql
        assert "alter column `email` unset tags ('pii', 'team')" in sql

    def test_unset_column_tags_noop_on_hive_metastore(self, passthrough_statement):
        passthrough_statement.relation.is_hive_metastore = lambda: True

        def boom(name):
            raise AssertionError("must not query column tags on hive metastore")

        passthrough_statement.context["load_result"] = boom
        columns = [DatabricksColumn("email", "string")]
        sql = self.run_macro(
            passthrough_statement.template,
            "unset_column_tags",
            passthrough_statement.relation,
            columns,
        )
        assert "unset tags" not in sql

    def test_unset_column_tags_noop_when_no_columns(self, passthrough_statement):
        def boom(name):
            raise AssertionError("must not query column tags when there is nothing to drop")

        passthrough_statement.context["load_result"] = boom
        sql = self.run_macro(
            passthrough_statement.template, "unset_column_tags", passthrough_statement.relation, []
        )
        assert "unset tags" not in sql
