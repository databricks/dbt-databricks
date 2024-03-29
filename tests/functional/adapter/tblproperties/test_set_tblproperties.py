from typing import List

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

    def check_tblproperties(self, project, model_name: str, properties: List[str]):
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
        util.check_relations_equal(
            project.adapter, ["set_tblproperties_to_view", "expected"]
        )

        self.check_tblproperties(
            project,
            "set_tblproperties",
            ["delta.autoOptimize.optimizeWrite", "delta.autoOptimize.autoCompact"],
        )
        self.check_tblproperties(
            project, "set_tblproperties_to_view", ["tblproperties_to_view"]
        )

        self.check_snapshot_results(project.adapter, num_rows=3)
        self.check_tblproperties(project, "my_snapshot", ["tblproperties_to_snapshot"])
