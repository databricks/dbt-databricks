import pytest
from dbt.tests import util

from dbt.adapters.databricks.column import DatabricksColumn
from dbt.adapters.databricks.relation import DatabricksRelation, DatabricksRelationType
from tests.functional.adapter.columns import fixtures
from tests.functional.adapter.fixtures import (
    RequiresDescribeAsJsonCapabilityMixin,
    fail_if_get_columns_as_json_called_macros,
)


# The HMS cluster profile uses hive_metastore, where get_columns_in_relation already
# takes the legacy path via is_hive_metastore() — before the foreign-table check
# matters. This test only validates the fix on UC profiles where JSON column
# metadata would otherwise be preferred.
@pytest.mark.skip_profile("databricks_cluster")
class TestForeignTableGetColumns(RequiresDescribeAsJsonCapabilityMixin):
    """Foreign tables must fetch columns without attempting DESCRIBE AS JSON."""

    @pytest.fixture(scope="class")
    def models(self):
        # Lakehouse Federation foreign tables aren't available in CI, so we
        # materialize a normal UC table and assert column fetch works when the
        # relation is typed as Foreign (see test method below).
        return {
            "foreign_table_source.sql": fixtures.foreign_table_source_model,
            "schema.yml": fixtures.foreign_table_source_schema,
        }

    @pytest.fixture(scope="class")
    def macros(self):
        return {"fail_if_get_columns_as_json_called.sql": fail_if_get_columns_as_json_called_macros}

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, project):
        util.run_dbt(["debug", "--connection"])
        util.run_dbt(["run"])

    @pytest.fixture(scope="class")
    def expected_columns(self):
        return [
            DatabricksColumn(column="id", dtype="bigint"),
            DatabricksColumn(column="name", dtype="string"),
        ]

    def test_foreign_table_get_columns_returns_expected_columns(self, project, expected_columns):
        # No real federated table in CI — reuse the managed table above but mark
        # the relation Foreign so get_columns_in_relation exercises that code path.
        foreign_relation = DatabricksRelation.create(
            database=project.database,
            schema=project.test_schema,
            identifier="foreign_table_source",
            type=DatabricksRelationType.Foreign,
        )

        with project.adapter.connection_named("_test"):
            actual_columns = project.adapter.get_columns_in_relation(foreign_relation)

        assert actual_columns == expected_columns
