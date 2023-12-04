import re
from mock import Mock
import pytest
from jinja2 import Environment, FileSystemLoader, PackageLoader, Template
from dbt.adapters.databricks.relation import DatabricksRelation


class TemplateBundle:
    def __init__(self, template, context, relation):
        self.template = template
        self.context = context
        self.relation = relation


class MacroTestBase:
    @pytest.fixture(autouse=True)
    def config(self, context) -> dict:
        local_config = {}
        context["config"].get = lambda key, default=None, **kwargs: local_config.get(key, default)
        return local_config

    @pytest.fixture(autouse=True)
    def var(self, context) -> dict:
        local_var = {}
        context["var"] = lambda key, default=None, **kwargs: local_var.get(key, default)
        return local_var

    @pytest.fixture(scope="class")
    def default_context(self) -> dict:
        context = {
            "validation": Mock(),
            "model": Mock(),
            "exceptions": Mock(),
            "config": Mock(),
            "statement": lambda r, caller: r,
            "adapter": Mock(),
            "var": Mock(),
            "return": lambda r: r,
        }

        return context

    @pytest.fixture(scope="class")
    def spark_env(self) -> Environment:
        return Environment(
            loader=PackageLoader("dbt.include.spark", "macros"),
            extensions=["jinja2.ext.do"],
        )

    @pytest.fixture(scope="class")
    def spark_template_names(self) -> list:
        return ["adapters.sql"]

    @pytest.fixture(scope="class")
    def spark_context(self, default_context, spark_env, spark_template_names) -> dict:
        return self.build_up_context(default_context, spark_env, spark_template_names)

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros"]

    @pytest.fixture(scope="class")
    def databricks_env(self, macro_folders_to_load) -> Environment:
        return Environment(
            loader=FileSystemLoader(
                [f"dbt/include/databricks/{folder}" for folder in macro_folders_to_load]
            ),
            extensions=["jinja2.ext.do"],
        )

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return []

    @pytest.fixture(scope="class")
    def databricks_context(self, spark_context, databricks_env, databricks_template_names) -> dict:
        if not databricks_template_names:
            return spark_context
        return self.build_up_context(spark_context, databricks_env, databricks_template_names)

    def build_up_context(self, context, env, template_names):
        new_context = context.copy()
        for template_name in template_names:
            template = env.get_template(template_name, globals=context)
            new_context.update(template.module.__dict__)

        return new_context

    @pytest.fixture
    def context(self, databricks_context) -> dict:
        return databricks_context.copy()

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        raise NotImplementedError("Must be implemented by subclasses")

    @pytest.fixture
    def template(self, template_name, context, databricks_env) -> Template:
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

    @pytest.fixture(scope="class")
    def relation(self):
        data = {
            "path": {
                "database": "some_database",
                "schema": "some_schema",
                "identifier": "some_table",
            },
            "type": None,
        }

        return DatabricksRelation.from_dict(data)

    @pytest.fixture
    def template_bundle(self, template, context, relation):
        context["model"].alias = relation.identifier
        return TemplateBundle(template, context, relation)

    def run_macro_raw(self, template, name, *args):
        return getattr(template.module, name)(*args)

    def run_macro(self, template, name, *args):
        value = self.run_macro_raw(template, name, *args)
        return re.sub(r"\s\s+", " ", value).strip()

    def render_bundle(self, template_bundle, name, *args):
        return self.run_macro(template_bundle.template, name, template_bundle.relation, *args)
