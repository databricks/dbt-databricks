import pytest

from dbt.tests.adapter.simple_snapshot.new_record_timestamp_mode import (
    _delete_sql,
    _invalidate_sql_statements,
    _ref_snapshot_sql,
    _seed_new_record_mode_statements,
    _snapshot_actual_sql,
    _snapshots_yml,
    _update_sql,
)
from dbt.tests.util import check_relations_equal, run_dbt


class TestDatabricksSnapshotNewRecordMode:
    @pytest.fixture(scope="class")
    def snapshots(self):
        return {"snapshot.sql": _snapshot_actual_sql}

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "snapshots.yml": _snapshots_yml,
            "ref_snapshot.sql": _ref_snapshot_sql,
        }

    @pytest.fixture(scope="class")
    def seed_new_record_modes(self):
        return [
            sql.replace("text", "string")
            .replace("TEXT", "STRING")
            .replace(" WITHOUT TIME ZONE", "")
            for sql in _seed_new_record_mode_statements
        ]

    @pytest.fixture(scope="class")
    def invalidate_sql_1(self):
        return _invalidate_sql_statements[0]

    @pytest.fixture(scope="class")
    def invalidate_sql_2(self):
        return _invalidate_sql_statements[1]

    @pytest.fixture(scope="class")
    def update_sql(self):
        return _update_sql.replace("text", "string")

    @pytest.fixture(scope="class")
    def delete_sql(self):
        return _delete_sql

    def test_snapshot_new_record_mode(
        self,
        project,
        seed_new_record_modes,
        invalidate_sql_1,
        invalidate_sql_2,
        update_sql,
    ):
        for sql in seed_new_record_modes:
            project.run_sql(sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        project.run_sql(invalidate_sql_1)
        project.run_sql(invalidate_sql_2)
        project.run_sql(update_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1

        check_relations_equal(project.adapter, ["snapshot_actual", "snapshot_expected"])

        project.run_sql(_delete_sql)

        results = run_dbt(["snapshot"])
        assert len(results) == 1
