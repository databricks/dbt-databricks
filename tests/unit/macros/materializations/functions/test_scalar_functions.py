from unittest.mock import Mock

import pytest

from tests.unit.macros.base import MacroTestBase


class ScalarFunctionTestBase(MacroTestBase):
    """Base class for scalar function tests with shared fixtures"""

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "scalar.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/functions", "macros"]

    @pytest.fixture
    def default_context(self) -> dict:
        """Override default context to add formatted_scalar_function_args_sql"""
        # Get the base context
        mock_adapter = Mock()
        mock_adapter.quote = lambda identifier: f"`{identifier}`"

        def mock_relation_create(database=None, schema=None, identifier=None, type=None):
            mock_relation = Mock()
            if database and schema and type == "table":
                mock_relation.render = Mock(return_value=f"{schema}.{identifier}")
            elif database and schema:
                mock_relation.render = Mock(return_value=f"`{database}`.`{schema}`.`{identifier}`")
            elif schema:
                mock_relation.render = Mock(return_value=f"{schema}.{identifier}")
            else:
                mock_relation.render = Mock(return_value=identifier)
            return mock_relation

        from dbt.adapters.databricks.column import DatabricksColumn

        mock_api = Mock(Column=DatabricksColumn)
        mock_api.Relation.create = mock_relation_create

        # Create mock model with arguments for formatted_scalar_function_args_sql
        mock_model = Mock()
        mock_model.arguments = []

        def formatted_scalar_function_args_sql():
            """Mock implementation of formatted_scalar_function_args_sql"""
            args = []
            for arg in mock_model.arguments:
                args.append(f"{arg.name} {arg.data_type}")
            return ", ".join(args)

        context = {
            "validation": Mock(),
            "model": mock_model,
            "exceptions": Mock(),
            "config": Mock(),
            "statement": lambda r, caller: r,
            "adapter": mock_adapter,
            "var": Mock(),
            "log": Mock(return_value=""),
            "return": lambda r: r,
            "is_incremental": Mock(return_value=False),
            "api": mock_api,
            "local_md5": lambda s: f"hash({s})",
            "formatted_scalar_function_args_sql": formatted_scalar_function_args_sql,
        }
        return context


class TestScalarFunctionSQL(ScalarFunctionTestBase):
    """Tests for SQL UDF macro generation"""

    def setup_model_for_sql_udf(self, context):
        """Configure model mock for SQL UDF"""
        context["model"].language = "sql"
        context["model"].compiled_code = "SELECT value * 2"
        context["model"].returns = Mock()
        context["model"].returns.data_type = "FLOAT"
        arg = Mock()
        arg.name = "value"
        arg.data_type = "FLOAT"
        context["model"].arguments = [arg]

    def test_sql_udf_signature(self, template_bundle):
        self.setup_model_for_sql_udf(template_bundle.context)
        sql = self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_create_replace_signature_sql",
            template_bundle.relation,
        )
        assert "language sql" in sql
        assert "create or replace function" in sql

    def test_sql_udf_body(self, template_bundle):
        self.setup_model_for_sql_udf(template_bundle.context)
        sql = self.run_macro(template_bundle.template, "databricks__scalar_function_body_sql")
        assert "return" in sql
        assert "select value * 2" in sql


class TestScalarFunctionPython(ScalarFunctionTestBase):
    """Tests for Python UDF macro generation"""

    def setup_model_for_python_udf(
        self,
        context,
        runtime_version="3.11",
        entry_point="entry",
        meta=None,
        **config,
    ):
        """Configure model mock for Python UDF"""
        context["model"].language = "python"
        context["model"].compiled_code = "return value * 2"
        context["model"].returns = Mock()
        context["model"].returns.data_type = "FLOAT"
        arg = Mock()
        arg.name = "value"
        arg.data_type = "FLOAT"
        context["model"].arguments = [arg]
        config_values = {
            "runtime_version": runtime_version,
            "entry_point": entry_point,
            "meta": meta or {},
            **config,
        }
        context["model"].config.get = lambda k, default=None: config_values.get(k, default)

    def test_python_udf_signature(self, template_bundle):
        self.setup_model_for_python_udf(template_bundle.context)
        sql = self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_create_replace_signature_python",
            template_bundle.relation,
        )
        assert "language python" in sql
        assert "create or replace function" in sql
        # Databricks does not support RUNTIME_VERSION or HANDLER clauses
        assert "runtime_version" not in sql
        assert "handler" not in sql

    def test_python_udf_body_dollar_quoting(self, template_bundle):
        self.setup_model_for_python_udf(template_bundle.context)
        sql = self.run_macro_raw(
            template_bundle.template, "databricks__scalar_function_body_python"
        )
        assert "$$" in sql
        assert "return value * 2" in sql

    def test_python_udf_full(self, template_bundle):
        self.setup_model_for_python_udf(template_bundle.context)
        sql = self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_python",
            template_bundle.relation,
        )
        assert "language python" in sql
        assert "runtime_version" not in sql
        assert "handler" not in sql
        assert "$$" in sql

    @pytest.mark.parametrize(
        "config",
        [
            {},
            {"packages": []},
            {"meta": {"packages": []}},
            {"packages": [], "meta": {"packages": []}},
            {"packages": None, "environment_version": None},
        ],
    )
    def test_python_udf_omits_environment_when_unset_or_empty(self, template_bundle, config):
        self.setup_model_for_python_udf(template_bundle.context, **config)

        sql = self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_python",
            template_bundle.relation,
        )

        assert "environment" not in sql

    @pytest.mark.parametrize(
        ("config", "environment"),
        [
            (
                {"packages": ["package-one"]},
                "dependencies = '[\"package-one\"]', environment_version = 'None'",
            ),
            (
                {"packages": "package-one==1.0"},
                "dependencies = '[\"package-one==1.0\"]', environment_version = 'None'",
            ),
            (
                {"packages": ["package-one", "/Volumes/main/test/vol/package-two.whl"]},
                'dependencies = \'["package-one", "/Volumes/main/test/vol/package-two.whl"]\','
                " environment_version = 'None'",
            ),
            (
                {"meta": {"packages": ["https://example.com/package-one.whl"]}},
                "dependencies = '[\"https://example.com/package-one.whl\"]',"
                " environment_version = 'None'",
            ),
            (
                {"packages": [], "meta": {"packages": ["meta-package"]}},
                "dependencies = '[\"meta-package\"]', environment_version = 'None'",
            ),
            (
                {"packages": ["package-one==1.0; extra == 'test'"]},
                "dependencies = '[\"package-one==1.0; extra == \\u0027test\\u0027\"]',"
                " environment_version = 'None'",
            ),
            (
                {"packages": ["package-one"], "environment_version": "4"},
                "dependencies = '[\"package-one\"]', environment_version = '4'",
            ),
            (
                {"packages": ["package-one"], "meta": {"environment_version": 4}},
                "dependencies = '[\"package-one\"]', environment_version = '4'",
            ),
            ({"environment_version": "4"}, "environment_version = '4'"),
        ],
    )
    def test_python_udf_renders_environment(self, template_bundle, config, environment):
        self.setup_model_for_python_udf(
            template_bundle.context, runtime_version=None, entry_point=None, **config
        )

        sql = self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_python",
            template_bundle.relation,
        )

        self.assert_sql_equal(
            f"""
            CREATE OR REPLACE FUNCTION `some_database`.`some_schema`.`some_table` (value FLOAT)
            RETURNS FLOAT
            LANGUAGE PYTHON
            ENVIRONMENT ({environment})
            AS
            $$
            return value * 2
            $$
            """,
            sql,
        )

    def test_python_udf_preserves_literal_unicode_escape_in_package(self, template_bundle):
        self.setup_model_for_python_udf(template_bundle.context, packages=["package\\u0027one"])

        sql = self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_python",
            template_bundle.relation,
        )

        assert "dependencies = '[\"package\\\\u0027one\"]'" in sql

    @pytest.mark.parametrize(
        ("key", "value"), [("packages", ["package-one"]), ("environment_version", "4")]
    )
    def test_python_udf_rejects_environment_config_in_both_locations(
        self, template_bundle, key, value
    ):
        self.setup_model_for_python_udf(template_bundle.context, meta={key: value}, **{key: value})
        expected_error = (
            f"`{key}` is configured in both `config.{key}` and `config.meta.{key}`; use only one."
        )
        template_bundle.context["exceptions"].raise_compiler_error.side_effect = RuntimeError(
            expected_error
        )

        with pytest.raises(RuntimeError, match="configured in both"):
            self.run_macro(
                template_bundle.template,
                "databricks__scalar_function_python",
                template_bundle.relation,
            )

        template_bundle.context["exceptions"].raise_compiler_error.assert_called_once_with(
            expected_error
        )

    def test_python_udf_warns_when_runtime_version_set(self, template_bundle):
        """Test that warning is emitted when runtime_version is explicitly configured"""
        self.setup_model_for_python_udf(template_bundle.context, runtime_version="3.11")
        self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_python",
            template_bundle.relation,
        )
        # exceptions.warn should have been called with runtime_version message
        calls = template_bundle.context["exceptions"].warn.call_args_list
        warn_messages = [str(c) for c in calls]
        assert any("runtime_version" in msg for msg in warn_messages), (
            f"Expected warning about runtime_version, got: {warn_messages}"
        )

    def test_python_udf_warns_when_entry_point_set(self, template_bundle):
        """Test that warning is emitted when entry_point is explicitly configured"""
        self.setup_model_for_python_udf(template_bundle.context, entry_point="my_handler")
        self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_python",
            template_bundle.relation,
        )
        # exceptions.warn should have been called with entry_point message
        calls = template_bundle.context["exceptions"].warn.call_args_list
        warn_messages = [str(c) for c in calls]
        assert any("entry_point" in msg for msg in warn_messages), (
            f"Expected warning about entry_point, got: {warn_messages}"
        )

    def setup_model_for_multi_arg_python_udf(self, context):
        """Configure model mock for a multi-argument Python UDF (price FLOAT, quantity INT)."""
        context["model"].language = "python"
        context["model"].compiled_code = "return price * quantity"
        context["model"].returns = Mock()
        context["model"].returns.data_type = "FLOAT"
        arg_price = Mock()
        arg_price.name = "price"
        arg_price.data_type = "FLOAT"
        arg_quantity = Mock()
        arg_quantity.name = "quantity"
        arg_quantity.data_type = "INT"
        context["model"].arguments = [arg_price, arg_quantity]
        config_values = {"runtime_version": None, "entry_point": None}
        context["model"].config.get = lambda k, default=None: config_values.get(k, default)

    def test_python_udf_multi_arg_signature(self, template_bundle):
        """Test that multi-arg Python UDF generates correct signature with both args."""
        self.setup_model_for_multi_arg_python_udf(template_bundle.context)
        sql = self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_create_replace_signature_python",
            template_bundle.relation,
        )
        assert "price float, quantity int" in sql
        assert "language python" in sql
        assert "create or replace function" in sql

    def test_python_udf_multi_arg_body(self, template_bundle):
        """Test that multi-arg Python UDF body contains dollar-quoting and code."""
        self.setup_model_for_multi_arg_python_udf(template_bundle.context)
        sql = self.run_macro_raw(
            template_bundle.template, "databricks__scalar_function_body_python"
        )
        assert "$$" in sql
        assert "return price * quantity" in sql

    def test_python_udf_multi_arg_full(self, template_bundle):
        """Test full multi-arg Python UDF SQL: signature + body, no runtime_version/handler."""
        self.setup_model_for_multi_arg_python_udf(template_bundle.context)
        sql = self.run_macro(
            template_bundle.template,
            "databricks__scalar_function_python",
            template_bundle.relation,
        )
        assert "price float, quantity int" in sql
        assert "language python" in sql
        assert "$$" in sql
        assert "runtime_version" not in sql
        assert "handler" not in sql
