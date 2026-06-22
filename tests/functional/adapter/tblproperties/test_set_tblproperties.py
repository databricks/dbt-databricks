import pytest
from dbt.tests import util

from tests.functional.adapter.tblproperties import fixtures


class TestTblproperties:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "set_tblproperties.sql": fixtures.tblproperties_sql,
            "set_tblproperties_to_view.sql": fixtures.view_tblproperties_sql,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot.sql": fixtures.snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "expected.csv": fixtures.seed_csv,
        }

    def check_tblproperties(self, project, model_name: str, properties: list[str]):
        results = util.run_sql_with_adapter(
            project.adapter,
            f"show tblproperties {project.test_schema}.{model_name}",
            fetch="all",
        )
        tblproperties = [result[0] for result in results]

        for prop in properties:
            assert prop in tblproperties

    def check_snapshot_results(self, adapter, num_rows: int):
        results = util.run_sql_with_adapter(
            adapter, "select * from {database}.{schema}.my_snapshot", fetch="all"
        )
        assert len(results) == num_rows

    def test_set_tblproperties(self, project):
        util.run_dbt(["seed"])
        util.run_dbt(["run"])
        util.run_dbt(["run"])
        util.run_dbt(["snapshot"])
        util.run_dbt(["snapshot"])

        util.check_relations_equal(project.adapter, ["set_tblproperties", "expected"])
        util.check_relations_equal(project.adapter, ["set_tblproperties_to_view", "expected"])

        self.check_tblproperties(
            project,
            "set_tblproperties",
            ["delta.autoOptimize.optimizeWrite", "delta.autoOptimize.autoCompact"],
        )
        self.check_tblproperties(project, "set_tblproperties_to_view", ["tblproperties_to_view"])

        self.check_snapshot_results(project.adapter, num_rows=3)
        self.check_tblproperties(project, "my_snapshot", ["tblproperties_to_snapshot"])


@pytest.mark.skip_profile("databricks_cluster")
class TestUniformIcebergTblproperties:
    @pytest.fixture(scope="class")
    def models(self):
        return {"uniform_iceberg.sql": fixtures.uniform_iceberg_sql}

    def test_uniform_properties_applied(self, project):
        # With use_managed_iceberg off (the default), an iceberg table is a Delta
        # table in UniForm mode, so the Delta->Iceberg compatibility properties are
        # added automatically rather than being declared in the model config.
        util.run_dbt(["run"])

        results = util.run_sql_with_adapter(
            project.adapter,
            f"show tblproperties {project.test_schema}.uniform_iceberg",
            fetch="all",
        )
        tblproperties = {result[0]: result[1] for result in results}

        assert tblproperties["delta.universalFormat.enabledFormats"] == "iceberg"
        assert tblproperties["delta.enableIcebergCompatV2"] == "true"
