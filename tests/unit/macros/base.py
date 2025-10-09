import re
from typing import Any
from unittest.mock import Mock

import pytest
from jinja2 import Environment, FileSystemLoader, PackageLoader, Template

from dbt.adapters.databricks.column import DatabricksColumn


class TemplateBundle:
    def __init__(self, template, context, relation):
        self.template = template
        self.context = context
        self.relation = relation


class MacroTestBase:
    @pytest.fixture(autouse=True)
    def config(self, context) -> dict:
        """
        Anything you put in this dict will be returned by config in the rendered template
        """
        local_config: dict[str, Any] = {}
        context["config"].get = lambda key, default=None, **kwargs: local_config.get(key, default)
        return local_config

    @pytest.fixture(autouse=True)
    def var(self, context) -> dict:
        """
        Anything you put in this dict will be returned by var in the rendered template
        """
        local_var: dict[str, Any] = {}
        context["var"] = lambda key, default=None, **kwargs: local_var.get(key, default)
        return local_var

    @pytest.fixture
    def default_context(self) -> dict:
        """
        This is the default context used in all tests.
        """
        context = {
            "validation": Mock(),
            "model": Mock(),
            "exceptions": Mock(),
            "config": Mock(),
            "statement": lambda r, caller: r,
            "adapter": Mock(),
            "var": Mock(),
            "log": Mock(return_value=""),
            "return": lambda r: r,
            "is_incremental": Mock(return_value=False),
            "api": Mock(Column=DatabricksColumn),
            "local_md5": lambda s: f"hash({s})",
        }
        return context

    @pytest.fixture
    def spark_env(self) -> Environment:
        """
        The environment used for rendering dbt-spark macros
        """
        return Environment(
            loader=PackageLoader("dbt.include.spark", "macros"),
            extensions=["jinja2.ext.do"],
        )

    @pytest.fixture
    def spark_template_names(self) -> list:
        """
        The list of Spark templates to load for the tests.
        Use this if your macro relies on macros defined in templates we inherit from dbt-spark.
        """
        return ["adapters.sql"]

    @pytest.fixture
    def spark_context(self, default_context, spark_env, spark_template_names) -> dict:
        """
        Adds all the requested Spark macros to the context
        """
        return self.build_up_context(default_context, spark_env, spark_template_names)

    @pytest.fixture
    def macro_folders_to_load(self) -> list:
        """
        This is a list of folders from which we look to load Databricks macro templates.
        All folders are relative to the dbt/include/databricks folder.
        Folders will be searched for in the order they are listed here, in case of name collisions.
        """
        return ["macros"]

    @pytest.fixture
    def databricks_env(self, macro_folders_to_load) -> Environment:
        """
        The environment used for rendering Databricks macros
        """
        return Environment(
            loader=FileSystemLoader(
                [f"dbt/include/databricks/{folder}" for folder in macro_folders_to_load]
            ),
            extensions=["jinja2.ext.do"],
        )

    @pytest.fixture
    def databricks_template_names(self) -> list:
        """
        The list of databricks templates to load for referencing imported macros in the
        tests. Do not include the template you specify in template_name.  Use this when you need a
        macro defined in a template other than the one you render for the test.

        Ex: If you are testing the python.sql template, you will also need to load ["adapters.sql"]
        """
        return []

    @pytest.fixture
    def databricks_context(self, spark_context, databricks_env, databricks_template_names) -> dict:
        """
        Adds all the requested Databricks macros to the context
        """
        if not databricks_template_names:
            return spark_context
        return self.build_up_context(spark_context, databricks_env, databricks_template_names)

    def build_up_context(self, context, env, template_names):
        """
        Adds macros from the supplied env and template names to the context.
        """
        new_context = context.copy()
        for template_name in template_names:
            template = env.get_template(template_name, globals=context)
            new_context.update(template.module.__dict__)

        return new_context

    @pytest.fixture
    def template_name(self) -> str:
        """
        The name of the Databricks template you want to test, not including the path.

        Example: "adapters.sql"
        """
        raise NotImplementedError("Must be implemented by subclasses")

    @pytest.fixture
    def template(self, template_name, databricks_context, databricks_env) -> Template:
        """
        This creates the template you will test against.
        You generally don't want to override this.
        """
        context = databricks_context.copy()
        current_template = databricks_env.get_template(template_name, globals=context)

        def dispatch(macro_name, macro_namespace=None, packages=None):
            if hasattr(current_template.module, f"databricks__{macro_name}"):
                return getattr(current_template.module, f"databricks__{macro_name}")
            elif f"databricks__{macro_name}" in context:
                return context[f"databricks__{macro_name}"]
            else:
                return context[f"spark__{macro_name}"]

        context["adapter"].dispatch = dispatch

        return current_template

    @pytest.fixture
    def context(self, template) -> dict:
        """
        Access to the context used to render the template.
        Modification of the context will work for mocking adapter calls, but may not work for
        mocking macros.
        If you need to mock a macro, see the use of is_incremental in default_context.
        """
        return template.globals

    @pytest.fixture
    def relation(self):
        relation = Mock()
        relation.database = "some_database"
        relation.schema = "some_schema"
        relation.identifier = "some_table"
        relation.render = Mock(return_value="`some_database`.`some_schema`.`some_table`")
        relation.without_identifier = Mock(return_value="`some_database`.`some_schema`")
        relation.type = "table"
        return relation

    @pytest.fixture
    def template_bundle(self, template, context, relation):
        """
        Bundles up the compiled template, its context, and a dummy relation.
        """
        context["model"].alias = relation.identifier
        return TemplateBundle(template, context, relation)

    def run_macro_raw(self, template, name, *args):
        """
        Run the named macro from a template, and return the rendered value.
        """
        return getattr(template.module, name)(*args)

    def run_macro(self, template, name, *args):
        """
        Run the named macro from a template, and return the rendered value.
        This version strips off extra whitespace and newlines.
        """
        return self.clean_sql(self.run_macro_raw(template, name, *args))

    def clean_sql(self, sql):
        """Helper method to normalize SQL for comparison"""

        sql = sql.lower()
        # Remove extra whitespace, newlines, and standardize spacing
        sql = re.sub(r"\s+", " ", sql).strip()
        # Normalize multiple spaces to single spaces
        sql = re.sub(r" +", " ", sql)
        sql = sql.replace("( ", "(")
        sql = sql.replace(" )", ")")
        return sql

    def render_bundle(self, template_bundle, name, *args):
        """
        Convenience method for macros that take a relation as a first argument.
        """
        return self.run_macro(template_bundle.template, name, template_bundle.relation, *args)

    def assert_sql_equal(self, expected, actual, msg=None):
        """Assert that two SQL strings are equal after normalization"""
        clean_expected = self.clean_sql(expected)
        clean_actual = self.clean_sql(actual)
        msg = msg or f"SQL doesn't match:\nExpected:\n{clean_expected}\n\nActual:\n{clean_actual}"
        assert clean_expected == clean_actual, msg
