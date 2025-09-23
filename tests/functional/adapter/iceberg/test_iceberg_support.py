import pytest

from dbt.artifacts.schemas.results import RunStatus
from dbt.tests import util
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
