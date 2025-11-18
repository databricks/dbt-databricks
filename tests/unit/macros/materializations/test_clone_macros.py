from unittest.mock import Mock

import pytest

from dbt.adapters.databricks import constants
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
            # TODO: will dispatch work here?
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