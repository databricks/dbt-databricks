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

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "+tblproperties": {
                    "default_property": "always",
                    "tblproperties_to_view": "false"  # will be overwritten by model config
                }
            }
        }

    @staticmethod
    def _check_tblproperties(project, model_name: str, tblproperties: dict[str, str]):
        results = util.run_sql_with_adapter(
            project.adapter,
            f"show tblproperties {project.test_schema}.{model_name}",
            fetch="all",
        )
        relation_tblproperties = {k: v for k, v in results}

        assert tblproperties.items() <= relation_tblproperties.items()

    @staticmethod
    def _check_snapshot_results(adapter, num_rows: int):
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

        self._check_tblproperties(
            project,
            "set_tblproperties",
            {"delta.autoOptimize.optimizeWrite": "true", "delta.autoOptimize.autoCompact": "true"},
        )
        self._check_tblproperties(
            project,
            "set_tblproperties_to_view",
            {"tblproperties_to_view": "true", "default_property": "always"})

        self._check_snapshot_results(project.adapter, num_rows=3)
        self._check_tblproperties(project, "my_snapshot", {"tblproperties_to_snapshot": "true"})
