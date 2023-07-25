import unittest
from unittest import mock
import re
from jinja2 import Environment, FileSystemLoader, PackageLoader


class TestMacros(unittest.TestCase):
    def setUp(self):
        self.parent_jinja_env = Environment(
            loader=PackageLoader("dbt.include.spark", "macros"),
            extensions=["jinja2.ext.do"],
        )
        self.jinja_env = Environment(
            loader=FileSystemLoader("dbt/include/databricks/macros"),
            extensions=["jinja2.ext.do"],
        )

        self.config = {}
        self.var = {}
        self.default_context = {
            "validation": mock.Mock(),
            "model": mock.Mock(),
            "exceptions": mock.Mock(),
            "config": mock.Mock(),
            "statement": lambda r, caller: r,
            "adapter": mock.Mock(),
            "var": mock.Mock(),
            "return": lambda r: r,
        }
        self.default_context["config"].get = lambda key, default=None, **kwargs: self.config.get(
            key, default
        )

        self.default_context["var"] = lambda key, default=None, **kwargs: self.var.get(key, default)

    def _get_template(self, template_filename, parent_context=None):
        parent_filename = parent_context or template_filename
        parent = self.parent_jinja_env.get_template(parent_filename, globals=self.default_context)
        self.default_context.update(parent.module.__dict__)

        return self.jinja_env.get_template(template_filename, globals=self.default_context)

    def _run_macro_raw(self, name, *args):
        def dispatch(macro_name, macro_namespace=None, packages=None):
            if hasattr(self.template.module, f"databricks__{macro_name}"):
                return getattr(self.template.module, f"databricks__{macro_name}")
            else:
                return self.default_context[f"spark__{macro_name}"]

        self.default_context["adapter"].dispatch = dispatch

        return getattr(self.template.module, name)(*args)

    def _run_macro(self, name, *args):
        value = self._run_macro_raw(name, *args)
        return re.sub(r"\s\s+", " ", value).strip()
