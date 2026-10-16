import os

import pytest
from dbt.tests import util

from tests.functional.adapter.materialized_view_tests import fixtures

LOCATION_KEY = "delta.universalFormat.compatibility.location"


@pytest.mark.dlt
@pytest.mark.skip_profile("databricks_cluster", "databricks_uc_cluster")
@pytest.mark.skipif(
    not os.environ.get("DBT_DATABRICKS_LOCATION_ROOT"),
    reason="Compatibility Mode needs an external location",
)
class TestCompatibilityModeRebuildKeepsView:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"compatibility_seed.csv": fixtures.compatibility_seed_csv}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "compatibility_safer_mv.sql": fixtures.compatibility_safer_mv,
            "compatibility_partitioned_mv.sql": fixtures.compatibility_partitioned_mv,
        }

    @pytest.fixture(scope="class")
    def location_root(self, unique_schema):
        return f"{os.environ['DBT_DATABRICKS_LOCATION_ROOT']}/compatibility_mv/{unique_schema}"

    @pytest.fixture(scope="class")
    def project_config_update(self, location_root):
        return {"vars": {"compatibility_location_root": location_root}}

    @pytest.fixture(scope="class", autouse=True)
    def seeded(self, project):
        util.run_dbt(["seed"])

    def assert_view_survived(self, project, location_root, name, partition_columns):
        relation = f"{project.database}.{project.test_schema}.{name}"
        table_type = project.run_sql(
            "select table_type from system.information_schema.tables"
            f" where table_catalog = '{project.database}'"
            f" and table_schema = '{project.test_schema}' and table_name = '{name}'",
            fetch="one",
        )
        assert table_type == ("MATERIALIZED_VIEW",)
        assert project.run_sql(f"select id, grp from {relation} order by id", fetch="all") == [
            (1, "a"),
            (2, "b"),
        ]
        properties = dict(project.run_sql(f"show tblproperties {relation}", fetch="all"))
        assert properties[LOCATION_KEY] == f"{location_root}/{name}"

        rows = project.run_sql(f"describe extended {relation}", fetch="all")
        names = [row[0] for row in rows]
        partitions = []
        if "# Partition Information" in names:
            for row in rows[names.index("# Partition Information") + 1 :]:
                if not row[0]:
                    break
                if not row[0].startswith("# "):
                    partitions.append(row[0])
        assert partitions == partition_columns

    def test_safer_full_refresh_replaces_view_in_place(self, project, location_root):
        util.run_dbt(["run", "--select", "compatibility_safer_mv"])
        util.run_dbt(["run", "--select", "compatibility_safer_mv", "--full-refresh"])

        self.assert_view_survived(project, location_root, "compatibility_safer_mv", [])

    def test_partition_removal_is_refused_before_drop(self, project, location_root):
        util.run_dbt(["run", "--select", "compatibility_partitioned_mv"])
        util.run_dbt(
            ["run", "--select", "compatibility_partitioned_mv", "--vars", '{"partition_by": []}'],
            expect_pass=False,
        )

        self.assert_view_survived(project, location_root, "compatibility_partitioned_mv", ["grp"])
