from unittest.mock import Mock, call

import pytest

from tests.unit.macros.base import MacroTestBase


def jinja_safe_mock(**kwargs):
    mock = Mock(**kwargs)
    mock.unsafe_callable = False
    mock.alters_data = False
    return mock


class TestReplaceWithMetricView(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "alter.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/relations/metric_view", "macros"]

    def test_caches_target_after_replacement(self, template_bundle, context):
        calls = Mock()
        for name, return_value in [
            ("get_replace_sql", ["REPLACE"]),
            ("execute_multiple_statements", ""),
            ("cache_replaced_relation", ""),
            ("apply_tags", ""),
        ]:
            context[name] = jinja_safe_mock(return_value=return_value)
            calls.attach_mock(context[name], name)
        context["sql"] = "version: 0.1"
        context["adapter"].clean_sql.return_value = "version: 0.1"
        existing = Mock()

        self.run_macro_raw(
            template_bundle.template,
            "replace_with_metric_view",
            existing,
            template_bundle.relation,
        )

        execution_calls = [
            c
            for c in calls.mock_calls
            if c[0] in ("execute_multiple_statements", "cache_replaced_relation")
        ]
        assert execution_calls == [
            call.execute_multiple_statements(["REPLACE"]),
            call.cache_replaced_relation(existing, template_bundle.relation),
        ]
