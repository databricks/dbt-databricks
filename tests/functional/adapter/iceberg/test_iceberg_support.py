import pytest
from dbt.artifacts.schemas.results import RunStatus
from dbt.tests import util

from tests.functional.adapter.fixtures import ManagedIcebergMixin
from tests.functional.adapter.iceberg import fixtures


def get_provider(project, identifier):
    rows = project.run_sql(
        f"describe extended {{database}}.{{schema}}.{identifier}",
        fetch="all",
    )
    for col_name, value, *_ in rows:
        if str(col_name).strip() == "Provider":
            return str(value).strip().lower()
    return None


def get_tblproperty(project, identifier, key):
    rows = project.run_sql(
        f"show tblproperties {{database}}.{{schema}}.{identifier}",
        fetch="all",
    )
    values = [row[1] for row in rows if row[0] == key]
    return values[0] if values else None


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

    def test_table_format_providers(self, project):
        # With use_managed_iceberg off, table_format="iceberg" produces a Delta
        # table with UniForm Iceberg metadata rather than a native Iceberg table.
        util.run_dbt()
        assert get_provider(project, "first_table") == "delta"
        assert get_provider(project, "iceberg_table") == "delta"
        uniform = get_tblproperty(project, "iceberg_table", "delta.universalFormat.enabledFormats")
        assert uniform is not None and "iceberg" in uniform


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


@pytest.mark.skip_profile("databricks_cluster")
class TestIcebergIncrementalPartitionClustering(ManagedIcebergMixin):
    """Regression test for https://github.com/databricks/dbt-databricks/issues/1495.

    Managed Iceberg normalizes `partition_by` into liquid clustering keys server-side.
    The clustering must survive the first incremental MERGE.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "iceberg_partition_inc.sql": fixtures.incremental_iceberg_partition_base,
            "schema.yml": fixtures.incremental_iceberg_partition_schema,
        }

    def _clustering_state(self, project, table="iceberg_partition_inc"):
        rows = project.run_sql(f"describe detail {table}", fetch="all")
        return " | ".join(str(cell) for cell in rows[0])

    def test_clustering_survives_incremental_run(self, project):
        util.run_dbt(["run"])
        initial = self._clustering_state(project)
        assert "business_date" in initial, (
            f"expected clustering on business_date after CREATE, got: {initial}"
        )

        util.write_file(
            fixtures.incremental_iceberg_partition_update,
            "models",
            "iceberg_partition_inc.sql",
        )
        util.run_dbt(["run"])

        final = self._clustering_state(project)
        assert "business_date" in final, (
            f"clustering was wiped after the incremental run; DESCRIBE DETAIL: {final}"
        )


class TestManagedIcebergTables(TestIcebergTables, ManagedIcebergMixin):
    def test_table_format_providers(self, project):
        # With use_managed_iceberg on, table_format="iceberg" produces a native
        # Iceberg table, not a UniForm-enabled Delta table.
        util.run_dbt()
        assert get_provider(project, "first_table") == "delta"
        assert get_provider(project, "iceberg_table") == "iceberg"
        assert (
            get_tblproperty(project, "iceberg_table", "delta.universalFormat.enabledFormats")
            is None
        )


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
