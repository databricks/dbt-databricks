import pytest
from dbt.tests import util
from dbt.tests.adapter.relations.test_changing_relation_type import (
    BaseChangeRelationTypeValidator,
)

from tests.functional.adapter.fixtures import (
    MaterializationV1Mixin,
    MaterializationV2Mixin,
    RerunSafeMixin,
)
from tests.functional.adapter.relations import fixtures


class TestChangeRelationTypesDatabricks(BaseChangeRelationTypeValidator):
    pass


class _TableToViewBase(RerunSafeMixin):
    """Materialize a model as a table, then switch it to a view WITHOUT --full-refresh.

    There is no handle_existing_table override in the adapter, so neither path raises; both
    drop/convert the table to a view. This pins that server-observable outcome on each flag.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {"flip_relation.sql": fixtures.flip_relation_as_table_sql}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("flip_relation",)

    def _relation_type(self, project):
        with project.adapter.connection_named("__test_check"):
            relation = project.adapter.get_relation(
                database=project.database,
                schema=project.test_schema,
                identifier="flip_relation",
            )
        return relation.type if relation is not None else None

    def _materialize_table_then_view(self, project):
        util.run_dbt(["run"])
        assert self._relation_type(project) == "table"
        util.write_file(fixtures.flip_relation_as_view_sql, "models", "flip_relation.sql")
        util.run_dbt(["run"])  # no --full-refresh


@pytest.mark.skip_profile("databricks_cluster")
class TestTableToViewWithoutFullRefreshV1(_TableToViewBase, MaterializationV1Mixin):
    def test_table_converted_to_view(self, project):
        self._materialize_table_then_view(project)
        assert self._relation_type(project) == "view"


@pytest.mark.skip_profile("databricks_cluster")
class TestTableToViewWithoutFullRefreshV2(_TableToViewBase, MaterializationV2Mixin):
    def test_table_converted_to_view(self, project):
        self._materialize_table_then_view(project)
        assert self._relation_type(project) == "view"


@pytest.mark.skip_profile("databricks_uc_cluster", "databricks_uc_sql_endpoint")
class TestChangeRelationTypesParquetDatabricks(BaseChangeRelationTypeValidator):
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+file_format": "parquet",
                "+incremental_strategy": "append",
            }
        }
