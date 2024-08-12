import pytest
from dbt.tests import util
from tests.functional.adapter.incremental import fixtures


class TestIncrementalTblproperties:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns_sql.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.tblproperties_a,
        }

    # For now, only add/change, no deletes
    def test_changing_tblproperties(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.tblproperties_b, "models", "schema.yml")
        util.run_dbt(["run"])
        results = project.run_sql(
            "show tblproperties {database}.{schema}.merge_update_columns_sql",
            fetch="all",
        )
        results_dict = {}
        for result in results:
            results_dict[result.key] = result.value
        assert results_dict["c"] == "e"
        assert results_dict["d"] == "f"


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalPythonTblproperties:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tblproperties.py": fixtures.simple_python_model,
            "schema.yml": fixtures.python_tblproperties_schema,
        }

    def test_changing_tblproperties(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.python_tblproperties_schema2, "models", "schema.yml")
        util.run_dbt(["run"])
        results = project.run_sql(
            "show tblproperties {database}.{schema}.tblproperties",
            fetch="all",
        )
        results_dict = {}
        for result in results:
            results_dict[result.key] = result.value
        assert results_dict["c"] == "e"
        assert results_dict["d"] == "f"
