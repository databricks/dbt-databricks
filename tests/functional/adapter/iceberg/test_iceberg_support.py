import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests import util

from tests.functional.adapter.fixtures import ManagedIcebergMixin
from tests.functional.adapter.iceberg import fixtures


@pytest.mark.skip_profile("databricks_cluster")
class TestIcebergTables:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "first_table.sql": fixtures.basic_table,
            "iceberg_table.sql": fixtures.basic_iceberg,
            "table_built_on_iceberg_table.sql": fixtures.ref_iceberg,
        }

    def test_iceberg_refs(self, project):
        # Run table materialization twice to verify atomic replacement
        util.run_dbt()
        run_results = util.run_dbt()
        assert len(run_results) == 3


@pytest.mark.skip_profile("databricks_cluster")
class TestIcebergSwap:
    @pytest.fixture(scope="class")
    def models(self):
        return {"first_model.sql": fixtures.basic_view}

    def test_iceberg_swaps(self, project):
        util.run_dbt()
        util.write_file(fixtures.basic_iceberg_swap, "models", "first_model.sql")
        run_results = util.run_dbt()
        assert len(run_results) == 1
        util.write_file(fixtures.basic_incremental_swap, "models", "first_model.sql")
        run_results = util.run_dbt()
        assert len(run_results) == 1


class InvalidIcebergConfig:
    def test_iceberg_failures(self, project):
        results = util.run_dbt(expect_pass=False)
        assert results.results[0].status == RunStatus.Error


@pytest.mark.skip_profile("databricks_cluster")
class TestIcebergView(InvalidIcebergConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"first_model.sql": fixtures.invalid_iceberg_view}


@pytest.mark.skip_profile("databricks_cluster")
class TestIcebergWithParquet(InvalidIcebergConfig):
    @pytest.fixture(scope="class")
    def models(self):
        return {"first_model.sql": fixtures.invalid_iceberg_format}


class TestManagedIcebergTables(TestIcebergTables, ManagedIcebergMixin):
    pass


class TestManagedIcebergSwap(TestIcebergSwap, ManagedIcebergMixin):
    pass


@pytest.mark.skip_profile("databricks_cluster")
class TestIcebergIncrementalMerge(ManagedIcebergMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"iceberg_incremental.sql": fixtures.incremental_iceberg_base}

    def test_iceberg_incremental_merge(self, project):
        results = util.run_dbt()
        assert len(results) == 1

        result = project.run_sql("select * from iceberg_incremental order by id", fetch="all")
        assert len(result) == 1
        assert result[0][0] == 1  # id
        assert result[0][1] == "initial"  # status

        util.write_file(fixtures.incremental_iceberg_update, "models", "iceberg_incremental.sql")

        # Second run should merge successfully without UniForm property errors
        results = util.run_dbt()
        assert len(results) == 1

        # Verify merged data: id=1 should be updated, id=2 should be new
        result = project.run_sql("select * from iceberg_incremental order by id", fetch="all")
        assert len(result) == 2
        assert result[0][0] == 1
        assert result[0][1] == "updated"  # Updated via merge
        assert result[1][0] == 2
        assert result[1][1] == "new"  # New row
