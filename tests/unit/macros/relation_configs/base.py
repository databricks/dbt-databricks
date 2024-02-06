from typing import Any
from jinja2 import Environment, FileSystemLoader
import pytest


class RelationConfigTestBase:
    @pytest.fixture(scope="class")
    def env(self) -> Environment:
        """
        The environment used for rendering Databricks macros
        """
        return Environment(
            loader=FileSystemLoader("dbt/include/databricks/macros/relations/components"),
            extensions=["jinja2.ext.do"],
        )

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        """
        The name of the Databricks template you want to test, not including the path.

        Example: "adapters.sql"
        """
        raise NotImplementedError("Must be implemented by subclasses")

    @pytest.fixture
    def template(self, template_name, env) -> Any:
        """
        This creates the template you will test against.
        You generally don't want to override this.
        """
        return env.get_template(template_name).module
