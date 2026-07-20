from unittest.mock import Mock

import pytest

from dbt.adapters.databricks import constants
from dbt.adapters.databricks.relation import DatabricksRelation
from tests.unit.macros.base import MacroTestBase
from tests.unit.utils import unity_relation


class TestCloneStrategies(MacroTestBase):
    t_location_root = "/mnt/root_dev/"

    def render_clone_macro(self, template_bundle, macro, s_relation, t_relation) -> str:
        external_path = f"{self.t_location_root}{template_bundle.relation.identifier}"
        adapter_mock = template_bundle.template.globals["adapter"]
        adapter_mock.compute_external_path.return_value = external_path
        return self.run_macro(
            template_bundle.template,
            macro,
            t_relation,
            s_relation,
        )

    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/clone", "macros/relations", "macros"]

    @pytest.fixture(scope="class")
    def databricks_template_names(self) -> list:
        return ["location.sql"]

    @pytest.fixture
    def s_relation(self):
        relation = Mock()
        relation.database = "some_database"
        relation.schema = "some_schema_prod"
        relation.identifier = "some_table"
        relation.render = Mock(return_value="`some_database`.`some_schema_prod`.`some_table`")
        relation.without_identifier = Mock(return_value="`some_database`.`some_schema_prod`")
        relation.type = "table"
        return relation

    @pytest.fixture
    def catalog_relation(self, template_bundle):
        t_relation = unity_relation(
            file_format=constants.DELTA_FILE_FORMAT,
            location_root=self.t_location_root,
            location_path=template_bundle.relation.identifier,
        )
        template_bundle.context["adapter"].build_catalog_relation.return_value = t_relation
        return t_relation

    def test_create_or_replace_clone(self, template_bundle, s_relation):
        sql = self.render_clone_macro(
            template_bundle,
            "databricks__create_or_replace_clone",
            s_relation,
            template_bundle.relation,
        )

        expected = self.clean_sql(
            "create or replace "
            f"table {template_bundle.relation.render()} "
            f"shallow clone {s_relation.render()}"
        )

        assert expected == sql

    def test_create_or_replace_clone_external(self, template_bundle, catalog_relation, s_relation):
        sql = self.render_clone_macro(
            template_bundle,
            "create_or_replace_clone_external",
            s_relation,
            template_bundle.relation,
        )

        expected = self.clean_sql(
            "create or replace "
            f"table {template_bundle.relation.render()} "
            f"shallow clone {s_relation.render()} "
            f"location '{catalog_relation.location}'"
        )

        assert expected == sql


class TestCloneRequiresDrop(MacroTestBase):
    @pytest.fixture(scope="class")
    def template_name(self) -> str:
        return "strategies.sql"

    @pytest.fixture(scope="class")
    def macro_folders_to_load(self) -> list:
        return ["macros/materializations/clone", "macros/relations", "macros"]

    def _relation(self, is_table=True, is_shallow_clone=False):
        relation = Mock(spec=DatabricksRelation)
        relation.is_table = is_table
        relation.is_shallow_clone = is_shallow_clone
        return relation

    def _requires_drop(self, template_bundle, existing_relation):
        rendered = self.run_macro_raw(
            template_bundle.template, "clone_requires_drop", existing_relation
        )
        return rendered.strip()

    def test_no_existing_relation_does_not_require_drop(self, template_bundle):
        assert self._requires_drop(template_bundle, None) == "False"

    def test_managed_table_requires_drop(self, template_bundle):
        # A regular table cannot become a shallow clone via `create or replace`.
        relation = self._relation(is_table=True, is_shallow_clone=False)
        assert self._requires_drop(template_bundle, relation) == "True"

    def test_view_requires_drop(self, template_bundle):
        relation = self._relation(is_table=False, is_shallow_clone=False)
        assert self._requires_drop(template_bundle, relation) == "True"

    def test_existing_shallow_clone_does_not_require_drop(self, template_bundle):
        # A clone->clone replace is a legal in-place `create or replace`.
        relation = self._relation(is_table=True, is_shallow_clone=True)
        assert self._requires_drop(template_bundle, relation) == "False"
