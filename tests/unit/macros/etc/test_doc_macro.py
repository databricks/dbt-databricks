from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class TestDocMacro(MacroTestBase):
    """Unit tests for the ``doc`` macro that bridges doc() into model rendering.

    See #1501: dbt-core only registers doc() in the schema-YAML context, so a
    metric_view body (rendered as model SQL) needs this macro fallback.
    """

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "doc.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/etc"]

    @pytest.fixture(autouse=True)
    def setup(self, context):
        # execute is False during parse; the macro resolves only at execution time.
        context["execute"] = True
        context["model"].package_name = "my_pkg"
        context["adapter"].resolve_doc = Mock(return_value="DOC CONTENTS")

    def test_single_arg_resolves_block(self, template_bundle):
        result = self.run_macro_raw(template_bundle.template, "doc", "my_block")

        assert result.strip() == "DOC CONTENTS"
        template_bundle.context["adapter"].resolve_doc.assert_called_once_with(
            "my_block", None, "my_pkg"
        )

    def test_two_args_pass_package(self, template_bundle):
        result = self.run_macro_raw(template_bundle.template, "doc", "other_pkg", "my_block")

        assert result.strip() == "DOC CONTENTS"
        template_bundle.context["adapter"].resolve_doc.assert_called_once_with(
            "my_block", "other_pkg", "my_pkg"
        )

    def test_unresolved_block_raises(self, template_bundle):
        template_bundle.context["adapter"].resolve_doc = Mock(return_value=None)

        self.run_macro_raw(template_bundle.template, "doc", "missing_block")

        template_bundle.context["exceptions"].raise_compiler_error.assert_called_once()

    # NOTE: the parse-time no-op (execute False) cannot be unit-tested here because
    # MacroTestBase mocks return() as a plain identity, so the macro's early
    # `{% if not execute %}{{ return('') }}` does not short-circuit rendering. That
    # path is exercised by the `dbt parse` functional path instead.
