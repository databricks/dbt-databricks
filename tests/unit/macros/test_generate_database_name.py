from types import SimpleNamespace

import pytest

from tests.unit.macros.base import MacroTestBase


class TestGenerateDatabaseNameMacro(MacroTestBase):
    """Precedence of databricks__generate_database_name:
    catalog_database > model `database` (custom_database_name) > catalog_name > target.database.
    """

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "get_custom_name/get_custom_database.sql"

    def _run(self, template_bundle, catalog_relation, custom_database_name=None, node=None):
        ctx = template_bundle.context
        ctx["target"] = {"database": "target_db"}
        ctx["adapter"].build_catalog_relation = lambda _node: catalog_relation
        return self.run_macro(
            template_bundle.template,
            "databricks__generate_database_name",
            custom_database_name,
            node,
        )

    def test_catalog_database_wins(self, template_bundle):
        # catalog_database takes precedence over everything, including a model `database`.
        relation = SimpleNamespace(catalog_database="prod_catalog", catalog_name="label_catalog")
        node = SimpleNamespace(database="model_db")
        assert self._run(template_bundle, relation, node=node) == "prod_catalog"
        assert (
            self._run(template_bundle, relation, custom_database_name="custom_db", node=node)
            == "prod_catalog"
        )

    def test_custom_database_beats_catalog_name(self, template_bundle):
        # No catalog_database: an explicit model `database` wins over catalog_name.
        relation = SimpleNamespace(catalog_database=None, catalog_name="name_catalog")
        node = SimpleNamespace(database="model_db")
        assert (
            self._run(template_bundle, relation, custom_database_name="custom_db", node=node)
            == "custom_db"
        )

    def test_catalog_name_when_no_catalog_database_or_custom(self, template_bundle):
        # No catalog_database, no custom database: fall back to catalog_name.
        relation = SimpleNamespace(catalog_database=None, catalog_name="name_catalog")
        node = SimpleNamespace(database="model_db")
        assert self._run(template_bundle, relation, node=node) == "name_catalog"

    def test_target_database_when_catalog_name_empty(self, template_bundle):
        # No catalog_database, no custom, empty catalog_name: fall back to target.database.
        relation = SimpleNamespace(catalog_database=None, catalog_name=None)
        node = SimpleNamespace(database="model_db")
        assert self._run(template_bundle, relation, node=node) == "target_db"
