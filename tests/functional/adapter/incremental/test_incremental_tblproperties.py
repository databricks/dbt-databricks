import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin
from tests.functional.adapter.incremental import fixtures


class TestIncrementalTblproperties(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("merge_update_columns_sql",)

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
        # Dropping a property from config does not unset it on the table; the
        # original value is left in place.
        assert results_dict["a"] == "b"


class TestIncrementalTblpropertiesBeforeWrite(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("merge_update_columns_sql",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "merge_update_columns_sql.sql": fixtures.merge_update_columns_sql,
            "schema.yml": fixtures.tblproperties_a,
        }

    def test_change_data_feed_enabled_on_incremental_run(self, project):
        util.run_dbt(["run"])
        util.write_file(fixtures.tblproperties_change_data_feed, "models", "schema.yml")
        util.run_dbt(["run"])
        relation = "{database}.{schema}.merge_update_columns_sql"
        merge_version = project.run_sql(
            f"select max(version) from (describe history {relation}) where operation = 'MERGE'",
            fetch="one",
        )[0]
        changes = project.run_sql(
            f"select id, _change_type from table_changes('{relation}', {merge_version},"
            f" {merge_version})",
            fetch="all",
        )
        assert sorted((row[0], row[1]) for row in changes) == [
            (2, "update_postimage"),
            (2, "update_preimage"),
            (3, "insert"),
        ]


@pytest.mark.python
@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalPythonTblproperties(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("tblproperties",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "tblproperties.py": fixtures.simple_python_model,
            "schema.yml": fixtures.python_tblproperties_schema,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"models": {"+create_notebook": "true"}}

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
        assert results_dict["a"] == "b"
