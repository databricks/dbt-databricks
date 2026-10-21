from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestGetMergeSQL(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    def render_update_set(
        self,
        template,
        update_columns=[],
        on_schema_change="ignore",
        source_columns=[],
        source_alias="src",
    ):
        return self.run_macro_raw(
            template,
            "get_merge_update_set",
            update_columns,
            on_schema_change,
            source_columns,
            source_alias,
        )

    def test_get_merge_update_set__update_columns(self, template):
        sql = self.render_update_set(template, update_columns=["a", "b", "c"], source_alias="s")
        expected = "`a` = s.`a`, `b` = s.`b`, `c` = s.`c`"
        self.assert_sql_equal(sql, expected)

    def test_get_merge_update_set__update_columns_takes_priority(self, template):
        sql = self.render_update_set(
            template,
            update_columns=["a"],
            on_schema_change="append",
            source_columns=["a", "b"],
            # source_alias is default
        )
        expected = "`a` = src.`a`"
        self.assert_sql_equal(sql, expected)

    def test_get_merge_update_set__no_update_columns_and_ignore(self, template):
        sql = self.render_update_set(
            template,
            update_columns=[],
            on_schema_change="ignore",
            source_columns=["a"],
            # source_alias is default
        )
        assert sql == "*"

    def test_get_merge_update_set__source_columns_and_not_ignore(self, template):
        sql = self.render_update_set(
            template,
            update_columns=[],
            on_schema_change="append",
            source_columns=["a", "b"],
            source_alias="SRC",
        )
        expected = "`a` = SRC.`a`, `b` = SRC.`b`"
        self.assert_sql_equal(sql, expected)

    def render_insert(
        self, template, on_schema_change="ignore", source_columns=[], source_alias="src"
    ):
        return self.run_macro_raw(
            template,
            "get_merge_insert",
            on_schema_change,
            source_columns,
            source_alias,
        )

    def test_get_merge_insert__ignore_takes_priority(self, template):
        # source_alias is default to 'src'
        sql = self.render_insert(template, on_schema_change="ignore", source_columns=["a"])
        assert sql == "*"

    def test_get_merge_insert__source_columns_and_not_ignore(self, template):
        # source_alias is default to 'src'
        sql = self.render_insert(template, on_schema_change="append", source_columns=["a", "b"])
        expected = "(`a`, `b`) VALUES (src.`a`, src.`b`)"
        self.assert_sql_equal(sql, expected)


class TestMergeActionsExplicit(MacroTestBase):
    """Tests for the merge_actions_explicit config key in databricks__get_merge_sql."""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/incremental"]

    @pytest.fixture(autouse=True)
    def setup_merge_context(self, context):
        """Set up adapter mocks required by databricks__get_merge_sql."""
        mock_col = Mock()
        mock_col.name = "id"
        mock_col.quoted = "`id`"
        context["adapter"].get_columns_in_relation.return_value = [mock_col]
        context["incremental_validate_on_schema_change"] = lambda val, default="ignore": (
            val if val else default
        )
        # These sibling macros from strategies.sql are called unqualified inside
        # databricks__get_merge_sql; inject minimal stubs so the macro can render.
        context["get_merge_update_columns"] = lambda *a, **kw: None
        context["get_merge_update_set"] = lambda *a, **kw: "*"
        context["get_merge_insert"] = lambda *a, **kw: "*"

    def render_merge_sql(
        self, template, config, unique_key="id", source="src_rel", target="tgt_rel"
    ):
        config.setdefault("target_alias", "DBT_INTERNAL_DEST")
        config.setdefault("source_alias", "DBT_INTERNAL_SOURCE")
        return self.run_macro_raw(
            template,
            "databricks__get_merge_sql",
            target,
            source,
            unique_key,
            [],  # dest_columns — overridden by adapter.get_columns_in_relation inside macro
            None,  # incremental_predicates
        )

    def test_explicit_actions_replace_default_when_clauses(self, template, config):
        """Explicit block is emitted verbatim and fully replaces the default clauses."""
        config["merge_actions_explicit"] = "when matched then FAKE_ACTION"
        sql = self.clean_sql(self.render_merge_sql(template, config))
        assert "when matched then fake_action" in sql
        # Default path renders "then update set *"; explicit block must replace it entirely
        assert "then update set" not in sql

    @pytest.mark.parametrize("value", [None, "", "   \n\t  "])
    def test_blank_explicit_actions_fall_back_to_default(self, template, config, value):
        """Null / empty / whitespace-only config falls back to the default path."""
        config["merge_actions_explicit"] = value
        sql = self.clean_sql(self.render_merge_sql(template, config))
        assert "when matched then update set" in sql
        assert "when not matched then insert" in sql
        assert "none" not in sql

    def test_no_conflicting_configs_no_warning(self, template, config, context):
        config["merge_actions_explicit"] = "when matched then FAKE_ACTION"
        self.render_merge_sql(template, config)
        context["exceptions"].warn.assert_not_called()

    @pytest.mark.parametrize(
        "key, value",
        [
            ("skip_matched_step", True),
            ("skip_matched_step", "true"),
            ("skip_not_matched_step", True),
            ("matched_condition", "1 = 1"),
            ("not_matched_condition", "1 = 1"),
            ("not_matched_by_source_action", "delete"),
            ("not_matched_by_source_condition", "1 = 1"),
        ],
    )
    def test_conflicting_config_warns(self, template, config, context, key, value):
        config["merge_actions_explicit"] = "when matched then FAKE_ACTION"
        config[key] = value
        self.render_merge_sql(template, config)
        context["exceptions"].warn.assert_called_once()
        assert context["exceptions"].warn.call_args.args[0].endswith(f": {key}")

    def test_multiple_conflicting_configs_listed_in_one_warning(self, template, config, context):
        config["merge_actions_explicit"] = "when matched then FAKE_ACTION"
        config["skip_not_matched_step"] = True
        config["matched_condition"] = "1 = 1"
        self.render_merge_sql(template, config)
        context["exceptions"].warn.assert_called_once()
        assert (
            context["exceptions"]
            .warn.call_args.args[0]
            .endswith(": skip_not_matched_step, matched_condition")
        )
