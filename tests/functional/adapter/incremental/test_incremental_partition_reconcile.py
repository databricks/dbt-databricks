"""partition_by is fixed at create and not reconciled on an incremental merge/append run
on either flag path (a layout change requires --full-refresh). Liquid clustering, by
contrast, IS reconciled (see test_incremental_clustering)."""

import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import (
    MaterializationV1Mixin,
    MaterializationV2Mixin,
    RerunSafeMixin,
)
from tests.functional.adapter.incremental import fixtures


def _partition_columns(project, identifier):
    rows = project.run_sql(f"describe detail {{database}}.{{schema}}.{identifier}", fetch="all")
    return rows[0]["partitionColumns"]


class _PartitionNotReconciledBase(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def models(self):
        return {"part_model.sql": fixtures.partitioned_incremental_initial_sql}

    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("part_model",)

    def _assert_partition_unchanged(self, project):
        util.run_dbt(["run"])
        assert _partition_columns(project, "part_model") == ["part_a"]
        util.write_file(fixtures.partitioned_incremental_changed_sql, "models", "part_model.sql")
        util.run_dbt(["run"])  # incremental append run, no --full-refresh
        after = _partition_columns(project, "part_model")
        assert after == ["part_a"], (
            f"partition_by must not be reconciled on an incremental append run, got {after}"
        )


@pytest.mark.skip_profile("databricks_cluster")
class TestPartitionNotReconciledV1(_PartitionNotReconciledBase, MaterializationV1Mixin):
    def test_partition_not_reconciled(self, project):
        self._assert_partition_unchanged(project)


@pytest.mark.skip_profile("databricks_cluster")
class TestPartitionNotReconciledV2(_PartitionNotReconciledBase, MaterializationV2Mixin):
    def test_partition_not_reconciled(self, project):
        self._assert_partition_unchanged(project)
