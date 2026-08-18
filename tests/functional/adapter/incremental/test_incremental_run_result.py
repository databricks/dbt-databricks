import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin
from tests.functional.adapter.incremental import fixtures


def _rows_affected(adapter_response):
    if isinstance(adapter_response, dict):
        return adapter_response.get("rows_affected")
    return getattr(adapter_response, "rows_affected", None)


@pytest.mark.skip_profile("databricks_cluster")
class TestIncrementalRunResult(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("incremental_run_result",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_run_result.sql": fixtures.incremental_run_result_sql,
        }

    def test_incremental_second_run_reports_rows_affected(self, project):
        util.run_dbt(["run", "--select", "incremental_run_result"])

        results = util.run_dbt(["run", "--select", "incremental_run_result"])
        assert len(results) == 1

        rows_affected = _rows_affected(results[0].adapter_response)
        if rows_affected is None:
            pytest.skip("Connector did not report rowcount for this incremental run")

        assert rows_affected > 0

        rows = project.run_sql(
            "select id, msg from incremental_run_result order by id", fetch="all"
        )
        assert [(row[0], row[1]) for row in rows] == [
            (1, "hello"),
            (2, "updated"),
            (3, "new"),
        ]
