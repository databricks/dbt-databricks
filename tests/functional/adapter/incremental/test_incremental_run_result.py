import pytest
from dbt.tests import util

from tests.functional.adapter.fixtures import RerunSafeMixin

incremental_run_result_sql = """
{{ config(
    materialized='incremental',
    unique_key='id',
    incremental_strategy='merge'
) }}

{% if not is_incremental() %}
select cast(1 as bigint) as id, 'hello' as msg
union all
select cast(2 as bigint) as id, 'goodbye' as msg
{% else %}
select cast(2 as bigint) as id, 'updated' as msg
union all
select cast(3 as bigint) as id, 'new' as msg
{% endif %}
"""


def _rows_and_message(adapter_response):
    if isinstance(adapter_response, dict):
        return adapter_response.get("rows_affected"), adapter_response.get("_message", "")
    return getattr(adapter_response, "rows_affected", None), str(adapter_response)


class TestIncrementalRunResult(RerunSafeMixin):
    @pytest.fixture(scope="class")
    def relations_to_reset(self):
        return ("incremental_run_result",)

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "incremental_run_result.sql": incremental_run_result_sql,
        }

    def test_incremental_second_run_reports_rows_affected(self, project):
        util.run_dbt(["run", "--select", "incremental_run_result"])

        results = util.run_dbt(["run", "--select", "incremental_run_result"])
        assert len(results) == 1

        rows_affected, message = _rows_and_message(results[0].adapter_response)
        if rows_affected is None:
            pytest.skip("Connector did not report rowcount for this incremental run")

        assert rows_affected > 0
        assert message.endswith(str(rows_affected))
